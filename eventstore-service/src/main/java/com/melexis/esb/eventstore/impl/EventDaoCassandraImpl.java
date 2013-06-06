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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.melexis.esb.eventstore.Event;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftColumnDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createIndexedSlicesQuery;

public class EventDaoCassandraImpl implements EventDao {

    public static final StringSerializer SERIALIZER = StringSerializer.get();
    public static final String LOTNAME = "LOTNAME";
    public static final String SOURCE = "SOURCE";
    public static final String PROCESSID = "PROCESSID";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final Function<Row<String,String,String>,Event> ROW_TO_EVENT_FN = new Function<Row<String, String, String>, Event>() {
        @Override
        public Event apply(@Nullable Row<String, String, String> row) {
            Map<String, String> attributes = new HashMap<String, String>();
            final List<HColumn<String, String>> columns = row.getColumnSlice().getColumns();
            String source = "";
            DateTime ts = null;
            for (HColumn<String, String> column : columns) {
                if (column.getName().equals(SOURCE)) {
                    source = column.getValue();
                } else if (column.getName().equals(TIMESTAMP)) {
                    ts = new DateTime(column.getValue());
                } else {
                    String key = column.getName();
                    String value = column.getValue();
                    attributes.put(key, value);
                }
            }

            final Event event = Event.createEvent(ts, source, attributes);
            return event;
        }
    };

    private final Keyspace keyspace;
    private final String columnFamily;
    private final int replicationFactor;

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public EventDaoCassandraImpl(Cluster cluster, String keyspaceName, String columnFamily) {
        this(cluster, keyspaceName, columnFamily, 3);
    }

    public EventDaoCassandraImpl(Cluster cluster, String keyspaceName, String columnFamily, int replicationFactor) {
        this.replicationFactor = replicationFactor;
        this.columnFamily = columnFamily;

        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);

        // If keyspace does not exist, the CFs don't exist either. => create them.
        // TODO: This is a blunt instrument. Needs refactoring when adding a new DAO.
        if (keyspaceDef == null) {
            createSchema(cluster, keyspaceName, columnFamily);
        }
        this.keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    }

    private void createSchema(Cluster cluster, String keyspace, String columnFamily) {

        List<ColumnDef> columns = new ArrayList<ColumnDef>();
        columns.add(newIndexedColumnDef(LOTNAME, "UTF8Type"));
        columns.add(newIndexedColumnDef(SOURCE, "UTF8Type"));
        columns.add(newIndexedColumnDef(PROCESSID, "UTF8Type"));
        columns.add(newIndexedColumnDef(TIMESTAMP, "UTF8Type"));
        List<ColumnDefinition> columnMetadata = ThriftColumnDef.fromThriftList(columns);

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace,
                columnFamily,
                ComparatorType.BYTESTYPE,
                columnMetadata);
        cfDef.setColumnType(ColumnType.STANDARD);


        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace,
                ThriftKsDef.DEF_STRATEGY_CLASS,
                replicationFactor,
                Arrays.asList(cfDef));

        // Add the schema to the cluster.
        cluster.addKeyspace(newKeyspace);
    }

    public void store(Event event) {

        ColumnFamilyTemplate<String, String> template
                = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily,
                SERIALIZER, SERIALIZER);

        ColumnFamilyUpdater<String, String> updater = template.createUpdater(UUID.randomUUID().toString());
        updater.setString(SOURCE, event.getSource());
        updater.setString(TIMESTAMP, event.getTimestamp().toString());
        for (Map.Entry<String, String> entry : event.getAttributes().entrySet()) {
            updater.setString(entry.getKey(), entry.getValue());
        }
        template.update(updater);

    }

    public List<Event> findEvents(String source, DateTime start, DateTime end, int max) {

        String from = (start == null) ? "" : start.toString();
        String till = (end == null) ? "" : end.toString();

        IndexedSlicesQuery<String, String, String> query =
                createIndexedSlicesQuery(keyspace, SERIALIZER, SERIALIZER, SERIALIZER);
        query.setColumnFamily(columnFamily);

        query.addEqualsExpression(SOURCE, source);

        addDateTimeConstraints(start, end, from, till, query);

        query.setRange("A", "z", false, 1000);

        List<Row<String, String, String>> rows = query.execute().get().getList();

        final List<Event> results = Lists.transform(rows, ROW_TO_EVENT_FN);

        return orderedResultSet(from, till, results, max);
    }



    @Override
    public List<Event> findEventsForLotnameAndSource(final String lotname,
                                                     final String source,
                                                     @Nullable DateTime start,
                                                     @Nullable DateTime end,
                                                     int max) {
        String from = (start == null) ? "" : start.toString();
        String till = (end == null) ? "" : end.toString();

        IndexedSlicesQuery<String, String, String> query =
                createIndexedSlicesQuery(keyspace, SERIALIZER, SERIALIZER, SERIALIZER);
        query.setColumnFamily(columnFamily);

        query.addEqualsExpression(LOTNAME, lotname);
        query.addEqualsExpression(SOURCE, source);
        query.setRange("A", "z", false, 1000);

        addDateTimeConstraints(start, end, from, till, query);

        final List<Row<String, String, String>> rows = query.execute().get().getList();
        final List<Event> results = Lists.transform(rows, ROW_TO_EVENT_FN);

        return orderedResultSet(from, till, results, max);
    }

    @Override
    public List<Event> findEventsForProcessIdAndSource(final String processId,
                                                       final String source,
                                                       @Nullable DateTime start,
                                                       @Nullable DateTime end,
                                                       int max) {
        String from = (start == null) ? "" : start.toString();
        String till = (end == null) ? "" : end.toString();

        IndexedSlicesQuery<String, String, String> query =
                createIndexedSlicesQuery(keyspace, SERIALIZER, SERIALIZER, SERIALIZER);
        query.setColumnFamily(columnFamily);

        query.addEqualsExpression(PROCESSID, processId);
        query.addEqualsExpression(SOURCE, source);
        query.setRange("A", "z", false, 1000);

        addDateTimeConstraints(start, end, from, till, query);

        final List<Row<String, String, String>> rows = query.execute().get().getList();
        final List<Event> results = Lists.transform(rows, ROW_TO_EVENT_FN);

        return orderedResultSet(from, till, results, max);
    }

    private final static void addDateTimeConstraints(DateTime start,
                                                     DateTime end,
                                                     String from,
                                                     String till,
                                                     IndexedSlicesQuery<String, String, String> query) {
        if ((till.compareTo(from) > 0) || till.equals("")) {
            query.addGteExpression(TIMESTAMP, from);
            if (end != null) {
                query.addLteExpression(TIMESTAMP, till);
            }
        } else {
            // return rows in reverse order
            if (start != null) {
                query.addGteExpression(TIMESTAMP, till);
            }
            query.addLteExpression(TIMESTAMP, from);
        }
    }

    private final static ColumnDef newIndexedColumnDef(String column_name, String comparer) {
        final StringSerializer ss = StringSerializer.get();
        final ColumnDef cd = new ColumnDef(ss.toByteBuffer(column_name), comparer);
        cd.setIndex_name(column_name);
        cd.setIndex_type(IndexType.KEYS);
        return cd;
    }

    private final static List<Event> orderedResultSet(String from, String till, List<Event> results, int max) {
        final int toIndex = max > results.size() ? results.size() : max;
        List<Event> events;
        if ((till.compareTo(from) > 0) || till.equals("")) {
            events = Ordering.natural().onResultOf(new Function<Event, Comparable>() {
                @Override
                public Comparable apply(@Nullable Event event) {
                    return event.getTimestamp();
                }
            }).sortedCopy(results);
        } else {
            events =  Ordering.natural().onResultOf(new Function<Event, Comparable>() {
                @Override
                public Comparable apply(@Nullable Event event) {
                    return event.getTimestamp();
                }
            }).reverse().sortedCopy(results);
        }
        return events.subList(0, toIndex);
    }
}

