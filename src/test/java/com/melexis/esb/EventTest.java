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

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static junit.framework.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: pti
 * Date: 9/21/11
 * Time: 8:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventTest {

    public static final String TEST_VALUE = "value";
    public static final String TEST_KEY = "test";
    DateTime TIMESTAMP = new DateTime("2010-02-12T12:34:56.789Z");
    String source = "SOURCE_ID";
    private HashMap<String,String> attributes;
    private Event ev;

    @Before
    public void setUp() {
        attributes = new HashMap<String, String>();
        attributes.put(TEST_KEY, TEST_VALUE);
        ev = Event.createEvent(TIMESTAMP, source, attributes);
    }

    @Test
    public void testCreateEvent() {
        assertEquals("Timestamps must match.", TIMESTAMP, ev.getTimestamp());
        assertEquals("Sources must match", source, ev.getSource());
        assertEquals("Attributes must match", attributes, ev.getAttributes());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateEventWithIllegalAttributes() {
        attributes.put("_illegal_","some value");
        ev = Event.createEvent(TIMESTAMP, source, attributes);
        fail("Should not get here after adding event with bs key.");
    }

    @Test
    public void testGet() {
        assertEquals("Attribute must match", TEST_VALUE, ev.get(TEST_KEY));
    }

    @Test
    public void testGetReservedKeys() {
        // reserved keys start with _
        assertEquals("Timestamp must match",TIMESTAMP.toString(), ev.get("_timestamp"));
        assertEquals("Sources must match",source,ev.get("_source"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIllegalKey() {
        // getting unsupported keys throw an exception
        String dummy = ev.get("_illegal_key");
        fail("Should not get here after getting bs key.");
    }

    @Test
    public void testEquals() {
        Event other = Event.createEvent(TIMESTAMP, source, attributes);
        assertEquals(ev,other);
    }

    @Test
    public void testCompareTo() {
        Event other = Event.createEvent(TIMESTAMP.plus(1000), source, attributes);
        assertTrue(ev.compareTo(other) < 0);
        assertTrue(other.compareTo(ev) > 0);
        assertEquals(0,ev.compareTo(ev));

        other = Event.createEvent(TIMESTAMP, source + "zz", attributes);
        assertTrue(ev.compareTo(other) < 0);
        assertTrue(other.compareTo(ev) > 0);
    }
}
