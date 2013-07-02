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

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Map;

/**
 * An immutable class to capture events happening.
 */
public class Event implements Comparable, Serializable {
    private DateTime timestamp;
    private String source;
    private Map<String,String> attributes;


    public Event(DateTime ts, String source, Map<String, String> attributes) {
        this.timestamp = ts;   // DateTime is immutable
        this.source = source;
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    public static Event createEvent(DateTime timestamp, String source, Map<String, String> attributes) {

        validateAttributes(attributes);
        return new Event(timestamp, source, attributes);

    }

    private static void validateAttributes(Map<String, String> attributes) {
        for(String key : attributes.keySet()) {
            if (key.startsWith("_")) {
                throw new IllegalArgumentException("Attributes passed to Event.createEvents " +
                        "may not contain reserved keys (starting with '_') like : " + key);
            }
        }
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public String getSource() {
        return source;
    }

    public Map<String,String> getAttributes() {
        return attributes;
    }

    public String get(String key) {
        if (!key.startsWith("_")) {
            return attributes.get(key);
        } else if(key.equals("_source")) {
            return getSource();
        } else if(key.equals("_timestamp")) {
            return getTimestamp().toString();
        } else {
            throw new IllegalArgumentException("Unrecognized reserved key value in Event.get : " + key);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (attributes != null ? !attributes.equals(event.attributes) : event.attributes != null) return false;
        if (source != null ? !source.equals(event.source) : event.source != null) return false;
        if (timestamp != null ? !timestamp.equals(event.timestamp) : event.timestamp != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = timestamp != null ? timestamp.hashCode() : 0;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    public int compareTo(Object o) {
        Event other = (Event)o;
        int result = getTimestamp().compareTo(other.getTimestamp());

        if (result == 0) {
            result = getSource().compareTo(other.getSource());
        }

        return result;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", source='" + source + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
