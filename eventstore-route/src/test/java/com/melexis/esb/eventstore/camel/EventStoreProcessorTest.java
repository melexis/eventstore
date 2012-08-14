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
import org.apache.camel.impl.DefaultMessage;
import org.apache.cassandra.net.MessagingService;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: pti
 * Date: 7/20/12
 * Time: 12:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventStoreProcessorTest {
    public static final String TEST_SOURCE = "TEST_SOURCE";
    private EventStoreProcessor processor;
    private static final DateTime TEST_DATE_TIME = new DateTime(2012,3,4,5,6,7,890);
    private static final DateTime OTHER_DATE_TIME = new DateTime(2011,2,3,4,5,6,789);
    private EventService service;
    private HashMap<String,String> attributes;
    private Event event;
    private Message in;
    private Exchange exchange;

    @Before
    public void setUp() throws Exception {
        processor = new EventStoreProcessor();
        processor.setDateTimeHelper(new DateTimeHelper() {
            @Override
            public DateTime now() {
                return TEST_DATE_TIME;
            }
        });

        service = mock(EventService.class);
        processor.setEventService(service);

        attributes = new HashMap<String,String>();
        event = new Event(TEST_DATE_TIME, TEST_SOURCE, attributes);

        in = new DefaultMessage();
        exchange = mock(Exchange.class);
        when(exchange.getIn()).thenReturn(in);

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testEventWithSourceInHeader() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(attributes);
        in.setBody(json);
        in.setHeader("Source",TEST_SOURCE);

        processor.process(exchange);

        verify(service).store(event);
    }

    @Test
    public void testEventWithSourceInBody() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        attributes.put("_source",TEST_SOURCE);
        String json = mapper.writeValueAsString(attributes);
        in.setBody(json);

        processor.process(exchange);

        verify(service).store(event);
    }

    @Test
    public void testEventWithTimeInBody() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        attributes.put("_ts",TEST_DATE_TIME.toString());
        String json = mapper.writeValueAsString(attributes);
        in.setBody(json);
        in.setHeader("Source",TEST_SOURCE);

        processor.setDateTimeHelper(new DateTimeHelper() {
            @Override
            public DateTime now() {
                return OTHER_DATE_TIME;
            }
        });

        processor.process(exchange);

        verify(service).store(event);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEventWithoutSource() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String json = mapper.writeValueAsString(attributes);
        in.setBody(json);

        processor.process(exchange);

        verify(service).store(event);
    }


}
