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

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

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
    EventDaoCassandraImpl dao;

    @Test
    public void testStore() {
        Event ev = createEvent(TEST_TS, TEST_SOURCE, 0);
        dao.store(ev);

        final List<HColumn<String, String>> columns = getColumns();

        // check first column
        HColumn<String, String> column1 = columns.get(0);
        assertEquals("key1", column1.getName());
        assertEquals("value1 - #0", column1.getValue());

        // check 2nd column
        HColumn<String, String> column2 = columns.get(1);
        assertEquals("key2", column2.getName());
        assertEquals("value2 - #0", column2.getValue());
    }

    private List<HColumn<String, String>> getColumns() {
        SuperColumnQuery<String, String, String, String> superColumnQuery =
                HFactory.createSuperColumnQuery(dao.getKeyspace(),
                        STRING_SERIALIZER,
                        STRING_SERIALIZER,
                        STRING_SERIALIZER,
                        STRING_SERIALIZER);

        superColumnQuery.setColumnFamily(dao.getColumnFamily())
                .setKey(TEST_SOURCE)
                .setSuperName(TEST_TS.toString());

        QueryResult<HSuperColumn<String, String, String>> result
                = superColumnQuery.execute();

        return result.get().getColumns();
    }


    private Event createEvent(DateTime ts, String s, int i) {
        Map<String, String> attr = new HashMap<String, String>();
        attr.put("key1", "value1 - #" + i);
        attr.put("key2", "value2 - #" + i);
        return Event.createEvent(ts, s, attr);
    }

    @Test
    public void testFindEvents() {
        initEvents();


        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, TEST_TS.plus(5 * INTERVAL_MS), 10000);

        assertEquals(6, events.size());

        // check the right events are returned in order
        for (int i = 0; i <= 5; i++) {
            Event ev = events.get(i);
            checkEvent(i, ev);
        }


    }

    private void initEvents() {
        for (int i = 0; i < NR_EVENTS; i++) {
            Event ev = createEvent(TEST_TS.plus(i * INTERVAL_MS), TEST_SOURCE, i);
            dao.store(ev);
        }
    }

    private void checkEvent(int i, Event ev) {
        assertEquals(TEST_TS.plus(i * INTERVAL_MS), ev.getTimestamp());
        assertEquals("value1 - #" + i, ev.get("key1"));
        assertEquals("value2 - #" + i,ev.get("key2"));
    }

    @Test
    public void testFindEventsLimited() {
        initEvents();


        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, TEST_TS.plus(5 * INTERVAL_MS), NR_LIMITED);

        assertEquals(NR_LIMITED, events.size());

        // check the right events are returned in order
        for (int i = 0; i < NR_LIMITED; i++) {
            Event ev = events.get(i);
            checkEvent(i, ev);
        }


    }

    @Test
    public void testFindEventsNoUpperBound() {
        initEvents();


        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS, null, 10000);

        assertEquals(NR_EVENTS, events.size());

    }


    @Test
    public void testFindEventsNoLowerBound() {
        initEvents();


        int nr = 50;
        List<Event> events = dao.findEvents(TEST_SOURCE,
                null, TEST_TS.plus(nr * INTERVAL_MS),
                10000);

        assertEquals(nr + 1, events.size());

    }

    @Test
    public void testFindEventsNoMatching() {

        // find all event strictly before the first one, i.e. none
        List<Event> events = dao.findEvents(TEST_SOURCE,
                null, TEST_TS.minus(INTERVAL_MS),
                10000);

        assertEquals(0, events.size());
    }

    @Test
    public void testFindEventsReverseOrder() {
        initEvents();

        List<Event> events = dao.findEvents(TEST_SOURCE, TEST_TS.plus(5 * INTERVAL_MS), TEST_TS, NR_LIMITED);

        assertEquals(NR_LIMITED, events.size());

        // check the right events are returned in REVERSE order
        for (int i = 0; i < NR_LIMITED; i++) {
            Event ev = events.get(i);
            checkEvent((5 - i), ev);
        }

    }


}

