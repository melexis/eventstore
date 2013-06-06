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

package com.melexis.esb.eventstore.impl;

import com.melexis.esb.eventstore.Event;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

public interface EventDao {

    void store(Event event);

    /**
     * Find events for the specified source.
     *
     * Find the events between from and till for the specified event source. The
     * from and till boundaries are inclusive.
     *
     * When a null is passed for the from parameter then all events before till
     * are returned.
     *
     * When a null is passed for the till parameter then all events after from
     * are returned.
     *
     * When both are null all events are returned for the source.
     *
     * When from is later than till, then the rows are returned in reverse order.
     *
     * The max parameter determines the number of events to return. A particular use
     * case is paging events on a screen. In this case return one more than the pagesize
     * and use its timestamp as the start of the next page.
     *
     * @param source  The id string of the source of the event
     * @param from    The earliest time to return events from
     * @param till    The latest moment to return events from
     * @param max     The maximum number of events to return
     * @return all events found, if any. Returns an empty list if no elements are found,
     */
    List<Event> findEvents(String source, @Nullable DateTime from, @Nullable DateTime till, int max);

    /**
     * Find all events for a given lotname.
     *
     * Find the events between from and till for the specified event source for a given lotname. The
     * from and till boundaries are inclusive.
     *
     * When a null is passed for the from parameter then all events before till
     * are returned.
     *
     * When a null is passed for the till parameter then all events after from
     * are returned.
     *
     * When both are null all events are returned for the source.
     *
     * When from is later than till, then the rows are returned in reverse order.
     *
     * The max parameter determines the number of events to return. A particular use
     * case is paging events on a screen. In this case return one more than the pagesize
     * and use its timestamp as the start of the next page.
     *
     * @param lotname
     * @param source
     * @param from
     * @param till
     * @param max
     * @return all events found, if any. Returns an empty list if no elements are found,
     */
    List<Event> findEventsForLotnameAndSource(final String lotname, final String source, @Nullable DateTime from, @Nullable DateTime till, int max);

    /**
     * Find all events for a given processId.
     *
     * Find the events between from and till for the specified event source for a given processId. The
     * from and till boundaries are inclusive.
     *
     * When a null is passed for the from parameter then all events before till
     * are returned.
     *
     * When a null is passed for the till parameter then all events after from
     * are returned.
     *
     * When both are null all events are returned for the source.
     *
     * When from is later than till, then the rows are returned in reverse order.
     *
     * The max parameter determines the number of events to return. A particular use
     * case is paging events on a screen. In this case return one more than the pagesize
     * and use its timestamp as the start of the next page.
     *
     * @param processId
     * @param source
     * @param start
     * @param end
     * @param max
     * @return all events found, if any. Returns an empty list if no elements are found,
     */
    List<Event> findEventsForProcessIdAndSource(final String processId,
                                                final String source,
                                                @Nullable DateTime start,
                                                @Nullable DateTime end,
                                                int max);

}
