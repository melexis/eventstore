/*
 * Copyright 2011 Melexis NV
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.melexis.esb;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.melexis.esb.eventstore.Event;
import com.melexis.esb.eventstore.impl.EventDaoCassandraImpl;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static me.prettyprint.hector.api.factory.HFactory.createIndexedSlicesQuery;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/beans.xml"})
public class EventDaoTest extends BaseCassandraTest {
    private static Logger log = LoggerFactory.getLogger(EventDaoTest.class);

    public static final DateTime TEST_TS = new DateTime("2010-01-02T12:34:56,789Z");
    public static final String TEST_SOURCE = "test_source";

    public static final int NR_EVENTS = 100;
    public static final int INTERVAL_MS = 5000;
    public static final int NR_LIMITED = 3;

    @Autowired
    Keyspace keyspace;

    @Autowired
    Cluster cluster;

    @Autowired
    EventDaoCassandraImpl dao;

    @Test @DirtiesContext
    public void testStore() {
        Event ev = createEvent(TEST_TS, TEST_SOURCE, 0);
        dao.store(ev);

        final List<HColumn<String, String>> columns = getColumns();



        // check first column
        HColumn<String, String> column1 = findColumnWithName("key1", columns);
        assertEquals("key1", column1.getName());
        assertEquals("value1 - #0", column1.getValue());

        // check 2nd column
        HColumn<String, String> column2 = findColumnWithName("key2", columns);
        assertEquals("key2", column2.getName());
        assertEquals("value2 - #0", column2.getValue());
    }

    private HColumn<String, String> findColumnWithName(final String name,
                                                       final List<HColumn<String, String>> columns) {
        return Iterables.filter(columns, new Predicate<HColumn<String, String>>() {
            @Override
            public boolean apply(@Nullable HColumn<String, String> col) {
                return col.getName().equals(name);
            }
        }).iterator().next();
    }

    private List<HColumn<String, String>> getColumns() {
        IndexedSlicesQuery<String, String, String> query =
                createIndexedSlicesQuery(dao.getKeyspace(), STRING_SERIALIZER, STRING_SERIALIZER, STRING_SERIALIZER);
        query.setColumnFamily(dao.getColumnFamily());

        query.addEqualsExpression(EventDaoCassandraImpl.SOURCE, TEST_SOURCE);
        query.addEqualsExpression(EventDaoCassandraImpl.TIMESTAMP, TEST_TS.toString());

        query.setRange("A", "z", false, 1000);

        final OrderedRows<String, String, String> res = query.execute().get();
        return res.getList().get(0).getColumnSlice().getColumns();
    }


    private Event createEvent(DateTime ts, String s, int i) {
        Map<String, String> attr = new HashMap<String, String>();
        attr.put("key1", "value1 - #" + i);
        attr.put("key2", "value2 - #" + i);
        return Event.createEvent(ts, s, attr);
    }

    @Test @DirtiesContext
    public void testFindEvents() {
        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, TEST_TS.plus(5 * INTERVAL_MS), 10000);

        assertEquals(6, events.size());

        // check the right events are returned in order
        for (int i = 0; i <= 5; i++) {
            Event ev = events.get(i);
            checkEvent(i, ev);
        }


    }

    @Before
    public void initEvents() {
        for (int i = 0; i < NR_EVENTS; i++) {
            Event ev = createEvent(TEST_TS.plus(i * INTERVAL_MS), TEST_SOURCE, i);
            dao.store(ev);
        }
    }

    @After
    public void cleanEvents() {
        cluster.truncate("EventStore", "Events");
    }

    private void checkEvent(int i, Event ev) {
        assertEquals(TEST_TS.plus(i * INTERVAL_MS), ev.getTimestamp());
        assertEquals("value1 - #" + i, ev.get("key1"));
        assertEquals("value2 - #" + i,ev.get("key2"));
    }

    @Test @DirtiesContext
    public void testFindEventsLimited() {
        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, TEST_TS.plus(5 * INTERVAL_MS), NR_LIMITED);

        System.out.println("EVENTS " + events);

        assertEquals(NR_LIMITED, events.size());

        // check the right events are returned in order
        for (int i = 0; i < NR_LIMITED; i++) {
            Event ev = events.get(i);
            checkEvent(i, ev);
        }


    }

    @Test @DirtiesContext
    public void testFindEventsNoUpperBound() {
        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, null, 10000);

        assertEquals(NR_EVENTS, events.size());

    }


    @Test @DirtiesContext
    public void testFindEventsNoLowerBound() {
        int nr = 50;
        List<Event> events = dao.findEvents(TEST_SOURCE,
                null, TEST_TS.plus(nr * INTERVAL_MS),
                10000);

        assertEquals(nr + 1, events.size());

    }

    @Test @DirtiesContext
    public void testFindEventsNoMatching() {

        // find all event strictly before the first one, i.e. none
        List<Event> events = dao.findEvents(TEST_SOURCE,
                null, TEST_TS.minus(INTERVAL_MS),
                10000);

        assertEquals(0, events.size());
    }

    @Test @DirtiesContext
    public void testFindEventsReverseOrder() {
        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS.plus(5 * INTERVAL_MS), TEST_TS, NR_LIMITED);

        assertEquals(NR_LIMITED, events.size());

        // check the right events are returned in REVERSE order
        for (int i = 0; i < NR_LIMITED; i++) {
            Event ev = events.get(i);
            checkEvent((5 - i), ev);
        }

    }

    @Test @DirtiesContext
    public void findEventsByLotname() {
        for (int i=0; i<50; i++) {
            dao.store(new Event(new DateTime(), "audit_log", ImmutableMap.of("LOTNAME", "A12345")));
        }

        List<Event> events = dao.findEventsForLotnameAndSource("A12345", "audit_log", null, null, 100);
        assertEquals(50, events.size());
    }

    @Test @DirtiesContext
    public void findEventsByLotnameLowerBounder() {
        final DateTime start = new DateTime();
        for (int i=0; i<50; i++) {
            dao.store(new Event(start.plusSeconds(i * 10), "audit_log", ImmutableMap.of("LOTNAME", "A12345")));
        }

        List<Event> events = dao.findEventsForLotnameAndSource("A12345", "audit_log", start.plusSeconds(30), null, 100);
        assertEquals(47, events.size());
    }
}

