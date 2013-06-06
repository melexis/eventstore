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

package com.melexis.esb.eventstore;

import com.melexis.esb.eventstore.Event;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface EventService {

    void store(Event event);

    void store(Date timestamp, String source, Map<String,String> attributes);
    void store(DateTime timestamp, String source, Map<String,String> attributes);
    void store(String isoTimestamp, String source, Map<String,String> attributes);
    void store(String source, Map<String,String> attributes);


    List<Event> findEvents(String source, DateTime from, DateTime till);
    List<Event> findEvents(String source, Date from, Date till);
    List<Event> findEvents(String source, String isoFrom, String isoTill);

    List<Event> findEvents(String source, DateTime from, DateTime till, int limit);
    List<Event> findEvents(String source, Date from, Date till, int limit);
    List<Event> findEvents(String source, String isoFrom, String isoTill, int limit);

    List<Event> findEvents(Collection<String> sources, DateTime from, DateTime till);
    List<Event> findEvents(Collection<String> sources, Date from, Date till);
    List<Event> findEvents(Collection<String> sources, String isoFrom, String isoTill);

    List<Event> findEvents(Collection<String> sources, DateTime from, DateTime till, int limit);
    List<Event> findEvents(Collection<String> sources, Date from, Date till, int limit);
    List<Event> findEvents(Collection<String> sources, String isoFrom, String isoTill, int limit);

    List<Event> findEventsForLotNameAndSource(final String lotname, final String source, int limit);

}
