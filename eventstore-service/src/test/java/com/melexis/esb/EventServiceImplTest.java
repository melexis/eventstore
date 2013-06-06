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

import com.google.common.collect.ImmutableList;
import com.melexis.esb.eventstore.Event;
import com.melexis.esb.eventstore.EventService;
import com.melexis.esb.eventstore.impl.EventDao;
import com.melexis.esb.eventstore.impl.EventServiceImpl;
import com.melexis.foundation.util.DateTimeHelper;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class EventServiceImplTest {

    public static final DateTime TEST_TS = new DateTime("2010-01-02T12:34:56,789Z");
    public static final DateTime TEST_TS_START = TEST_TS;
    public static final DateTime TEST_TS_END = TEST_TS_START.plus(100000);
    public static final int NR_LIMIT = 10000;
    public static final int DEFAULT_LIMIT = EventServiceImpl.DEFAULT_LIMIT;
    public static final int DT = 5000;
    public static final String TEST_SOURCE = "test-source";
    public static final String TEST_SOURCE_1 = "test-source-1";
    public static final String TEST_SOURCE_2 = "test-source-2";
    public static final String TEST_SOURCE_3 = "test-source-3";

    private EventService eventService;
    private EventDao eventDao;
    private HashMap<String, String> attributes;
    private Event event;
    private DateTimeHelper dateTimeHelper;
    private List<Event> eventList;

    private List<Event> eventList1;
    private List<Event> eventList2;
    private List<Event> eventList3;
    private List<Event> eventListSummed;
    private List<String> sources;

    @Before
    public void setUp() {
        eventDao = mock(EventDao.class);
        dateTimeHelper = mock(DateTimeHelper.class);
        eventService = new EventServiceImpl(eventDao, dateTimeHelper);

        attributes = new HashMap<String, String>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");

        event = Event.createEvent(TEST_TS, TEST_SOURCE, attributes);
        eventList = ImmutableList.of(event);


        createEventLists();
    }

    private void createEventLists() {
        eventList1 = new ArrayList<Event>();
        eventList2 = new ArrayList<Event>();
        eventList3 = new ArrayList<Event>();

        for (int i = 1; i <= 5; i++) {
            DateTime t1 = TEST_TS.plus(i * DT);
            DateTime t2 = t1.plus(DT);
            DateTime t3 = t2.plus(DT);

            eventList1.add(Event.createEvent(t1, TEST_SOURCE_1, attributes));
            eventList2.add(Event.createEvent(t2, TEST_SOURCE_2, attributes));
            eventList3.add(Event.createEvent(t3, TEST_SOURCE_3, attributes));
        }

        sources = ImmutableList.of(TEST_SOURCE_1, TEST_SOURCE_2, TEST_SOURCE_3);

        eventListSummed = new ArrayList<Event>(eventList1);
        eventListSummed.addAll(eventList2);
        eventListSummed.addAll(eventList3);
        Collections.sort(eventListSummed, new Comparator<Event>() {
            public int compare(Event event, Event event1) {
                return event.getTimestamp().compareTo(event1.getTimestamp());
            }
        });
    }

    @Test
    public void testStore() {
        eventService.store(event);

        verify(eventDao).store(event);
    }

    @Test
    public void testStoreWithDate() {
        eventService.store(TEST_TS.toDate(), TEST_SOURCE, attributes);
        verify(eventDao).store(event);
    }

    @Test
    public void testStoreWithDateTime() {
        eventService.store(TEST_TS, TEST_SOURCE, attributes);
        verify(eventDao).store(event);
    }

    @Test
    public void testStoreWithoutDateTime() {
        when(dateTimeHelper.now()).thenReturn(TEST_TS);

        eventService.store(TEST_SOURCE, attributes);

        verify(eventDao).store(event);
    }

    @Test
    public void testStoreJson() {
        String json = "{ts: \"" + TEST_TS
                + "\", source:\"" + TEST_SOURCE
                + "\", [";

        for(Map.Entry e : attributes.entrySet()) {
            json += "{ " + e.getKey() + ":\"" + e.getValue() + "\" }";
        }
    }

    @Test
    public void testFindEventsWithLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT);

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT);
    }

    @Test
    public void testFindEventsWithoutLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END);

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
    }

    @Test
    public void testFindEventsByDateWithLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START.toDate(), TEST_TS_END.toDate(), NR_LIMIT);

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT);
    }

    @Test
    public void testFindEventsByDateWithoutLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START.toDate(), TEST_TS_END.toDate());

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
    }

    @Test
    public void testFindEventsByIsoDateWithLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START.toDate(), TEST_TS_END.toDate(), NR_LIMIT);

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, NR_LIMIT);
    }

    @Test
    public void testFindEventsByIsoDateWithoutLimit() {
        when(eventDao.findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList);

        eventService.findEvents(TEST_SOURCE, TEST_TS_START.toString(), TEST_TS_END.toString());

        verify(eventDao).findEvents(TEST_SOURCE, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
    }

    @Test
    public void testFindEventsForSources() {
        when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList1);
        when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList2);
        when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList3);

        List<Event> result = eventService.findEvents(sources, TEST_TS_START, TEST_TS_END);

        verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);

        assertEquals(eventListSummed, result);
    }

    @Test
    public void testFindEventsForSourcesWithLimit() {
        int max = 3;
        when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList1);
        when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList2);
        when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList3);

        List<Event> result = eventService.findEvents(sources, TEST_TS_START, TEST_TS_END, max);

        verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max);
        verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max);
        verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max);

        assertEquals(3,result.size());
        assertEquals(eventListSummed.subList(0,3), result);
    }

    @Test
    public void testFindEventsForSourcesWithDates() {
        when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList1);
        when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList2);
        when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList3);

        List<Event> result = eventService.findEvents(sources, TEST_TS_START.toDate(), TEST_TS_END.toDate());

        verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);

        assertEquals(eventListSummed, result);
    }

    @Test
     public void testFindEventsForSourcesWithDatesAndLimit() {
         int max = 3;
         when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList1);
         when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList2);
         when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList3);

         List<Event> result = eventService.findEvents(sources, TEST_TS_START.toDate(), TEST_TS_END.toDate(), max);

         verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max);
         verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max);
         verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max);

         assertEquals(3,result.size());
         assertEquals(eventListSummed.subList(0,3), result);
     }

    @Test
    public void testFindEventsForSourcesWithIsoDates() {
        when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList1);
        when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList2);
        when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT)).thenReturn(eventList3);

        List<Event> result = eventService.findEvents(sources, TEST_TS_START.toString(), TEST_TS_END.toString());

        verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);
        verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, DEFAULT_LIMIT);

        assertEquals(eventListSummed, result);
    }

    @Test
     public void testFindEventsForSourcesWithIsoDatesAndLimit() {
         int max = 3;
         when(eventDao.findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList1);
         when(eventDao.findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList2);
         when(eventDao.findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max)).thenReturn(eventList3);

         List<Event> result = eventService.findEvents(sources, TEST_TS_START.toString(), TEST_TS_END.toString(), max);

         verify(eventDao).findEvents(TEST_SOURCE_1, TEST_TS_START, TEST_TS_END, max);
         verify(eventDao).findEvents(TEST_SOURCE_2, TEST_TS_START, TEST_TS_END, max);
         verify(eventDao).findEvents(TEST_SOURCE_3, TEST_TS_START, TEST_TS_END, max);

         assertEquals(3,result.size());
         assertEquals(eventListSummed.subList(0,3), result);
     }

    @Test
    public void findEventsForLotNameAndSource() {
        final String lotname = "A12345";
        final String source = "ewafermap";
        final int limit = 100;

        when(eventDao.findEventsForLotnameAndSource(lotname, source, null, null, limit)).thenReturn(eventList1);

        List<Event> events = eventService.findEventsForLotNameAndSource(lotname, source, limit);

        assertEquals(eventList1, events);
    }

    @Test
    public void findEventsForProcessIdAndSource() {
        final String processId = "blaat";
        final String source = "ewafermap";
        final int limit = 100;

        when(eventDao.findEventsForProcessIdAndSource(processId, source, null, null, limit)).thenReturn(eventList1);

        List<Event> events = eventService.findEventsForProcessIdAndSource(processId, source, limit);

        assertEquals(eventList1, events);
    }
}
