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
import com.melexis.util.DateTimeHelper;
import org.joda.time.DateTime;

import java.util.*;


public class EventServiceImpl implements EventService {

    public static final int DEFAULT_LIMIT = 1000000;

    private EventDao eventDao;
    private DateTimeHelper dateTimeHelper;

    public EventServiceImpl(EventDao eventDao, DateTimeHelper dateTimeHelper) {
        this.eventDao = eventDao;
        this.dateTimeHelper = dateTimeHelper;
    }

    public void store(Event event) {
        eventDao.store(event);
    }

    public void store(Date timestamp, String source, Map<String, String> attributes) {
        Event event = Event.createEvent(new DateTime(timestamp), source, attributes);
        store(event);
    }

    public void store(DateTime timestamp, String source, Map<String, String> attributes) {
        Event event = Event.createEvent(timestamp, source, attributes);
        store(event);
    }

    public void store(String isoTimestamp, String source, Map<String, String> attributes) {
        Event event = Event.createEvent(new DateTime(isoTimestamp), source, attributes);
        store(event);
    }

    public void store(String source, Map<String, String> attributes) {
        Event event = Event.createEvent(dateTimeHelper.now(), source, attributes);
        store(event);
    }

    public List<Event> findEvents(String source, DateTime from, DateTime till, int limit) {
        return eventDao.findEvents(source, from, till, limit);
    }

    public List<Event> findEvents(String source, Date from, Date till, int limit) {
        return findEvents(source, new DateTime(from), new DateTime(till), limit);
    }

    public List<Event> findEvents(String source, String isoFrom, String isoTill, int limit) {
        return findEvents(source, new DateTime(isoFrom), new DateTime(isoTill), limit);
    }

    public List<Event> findEvents(String source, DateTime from, DateTime till) {
        return findEvents(source, from, till, DEFAULT_LIMIT);
    }

    public List<Event> findEvents(String source, Date from, Date till) {
        return findEvents(source, from, till, DEFAULT_LIMIT);
    }

    public List<Event> findEvents(String source, String isoFrom, String isoTill) {
        return findEvents(source, isoFrom, isoTill, DEFAULT_LIMIT);
    }

    public List<Event> findEvents(Collection<String> sources, DateTime from, DateTime till) {
        return null;
    }

    public List<Event> findEvents(Collection<String> sources, Date from, Date till) {
        return null;
    }

    public List<Event> findEvents(Collection<String> sources, String isoFrom, String isoTill) {
        return null;
    }

    public List<Event> findEvents(Collection<String> sources, DateTime from, DateTime till, int limit) {
        SortedSet<Event> events = new TreeSet<Event>();
        for(String source : sources) {
            events.addAll(findEvents(source,from,till,limit));
        }
        return ImmutableList.copyOf(events);
    }

    public List<Event> findEvents(Collection<String> sources, Date from, Date till, int limit) {
        return null;
    }

    public List<Event> findEvents(Collection<String> sources, String isoFrom, String isoTill, int limit) {
        return null;
    }

}
