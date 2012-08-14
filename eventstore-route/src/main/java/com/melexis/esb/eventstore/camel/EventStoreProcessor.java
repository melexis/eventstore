/*
 * Copyright 2012 Melexis NV
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

package com.melexis.esb.eventstore.camel;

import com.melexis.esb.eventstore.Event;
import com.melexis.esb.eventstore.EventService;
import com.melexis.foundation.util.DateTimeHelper;
import org.apache.camel.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: pti
 * Date: 7/20/12
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventStoreProcessor implements Processor {

    public static final String SOURCE_KEY = "_source";
    public static final String TS_KEY = "_ts";
    private EventService eventService;
    private DateTimeHelper dateTimeHelper;

    private ObjectMapper mapper = new ObjectMapper();

    private static final TypeReference<Map<String, String>> VALUE_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    @Override
    public void process(Exchange exchange) throws Exception {
        Message in = exchange.getIn();
        String eventJson = (String) in.getBody();

        // source header overrides data
        String source = (String) in.getHeader("Source");

        Map<String,String> attributes = mapper.readValue(eventJson, VALUE_TYPE_REF);

        // fall back to reserved attribute _source
        if ((source == null) && attributes.containsKey(SOURCE_KEY)) {
            source = attributes.get(SOURCE_KEY);
            attributes.remove(SOURCE_KEY);
        }

        if (source == null) {
            throw new IllegalArgumentException("Cannot log events of unknown sources. Add " +
                    "a '_source' attribute to the event data or set the 'Source' header " +
                    "on the message exchange.");
        }

        DateTime timeStamp = dateTimeHelper.now();
        if (attributes.containsKey(TS_KEY)) {
            String ts = attributes.get(TS_KEY);
            timeStamp = dateTimeHelper.parse(ts);
            attributes.remove(TS_KEY);
        }

        Event event = Event.createEvent(timeStamp,source,attributes);
        eventService.store(event);

    }

    public void setEventService(EventService eventService) {
        this.eventService = eventService;
    }

    public void setDateTimeHelper(DateTimeHelper dateTimeHelper) {
        this.dateTimeHelper = dateTimeHelper;
    }
}
