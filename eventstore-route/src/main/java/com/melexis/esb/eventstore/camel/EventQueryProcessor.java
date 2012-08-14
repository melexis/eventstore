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
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.http.HttpMessage;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: pti
 * Date: 7/20/12
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventQueryProcessor implements Processor {

    public static final String SOURCE_KEY = "_source";
    public static final String TS_KEY = "_ts";
    public static final int DEFAULT_LIMIT = 1000;
    private EventService eventService;
    private DateTimeHelper dateTimeHelper;

    private ObjectMapper mapper = new ObjectMapper();


    public EventQueryProcessor() {
        mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    private static final TypeReference<Map<String, String>> VALUE_TYPE_REF =
            new TypeReference<Map<String, String>>() {
            };
    private String urlPrefix;

    @Override
    public void process(Exchange exchange) throws Exception {
        Message in = exchange.getIn();
        Message out = exchange.getOut();

        String source = getSource(in);
        DateTime from = dateTimeHelper.parse((String) in.getHeader("from"));
        DateTime till = dateTimeHelper.parse((String) in.getHeader("till"));
        int limit = DEFAULT_LIMIT;
        if (in.getHeader("limit") != null) {
            limit = Integer.parseInt((String) in.getHeader("limit"));
        }
        List<Event> events = eventService.findEvents(source, from, till, limit);

        out.setHeader("Source", source);
        out.setHeader("from", dateTimeHelper.format(from));
        out.setHeader("till", dateTimeHelper.format(till));
        out.setHeader("limit", limit);
        out.setBody(mapper.writeValueAsString(events));

    }

    private String getSource(Message in) {
        String source = (String) in.getHeader("Source");
        HttpServletRequest req = in.getBody(HttpServletRequest.class);
        if (req != null) {
            source = req.getPathInfo().replaceFirst(urlPrefix + "/", "");
        }
        return source;
    }

    public void setEventService(EventService eventService) {
        this.eventService = eventService;
    }

    public void setDateTimeHelper(DateTimeHelper dateTimeHelper) {
        this.dateTimeHelper = dateTimeHelper;
    }

    public void setUrlPrefix(String urlprefix) {
        this.urlPrefix = urlprefix;
    }
}
