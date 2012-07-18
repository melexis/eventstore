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
import com.melexis.esb.eventstore.Event;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.SuperCfTemplate;
import me.prettyprint.cassandra.service.template.SuperCfUpdater;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SuperSliceQuery;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static me.prettyprint.hector.api.factory.HFactory.createSuperSliceQuery;

public class EventDaoCassandraImpl implements EventDao {

    public static final StringSerializer SERIALIZER = StringSerializer.get();

    Keyspace keyspace;
    String columnFamily;

    private int replicationFactor = 1;

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public EventDaoCassandraImpl(Cluster cluster, String keyspaceName, String columnFamily) {
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

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace,
                columnFamily,
                ComparatorType.BYTESTYPE);
        cfDef.setColumnType(ColumnType.SUPER);

        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace,
                ThriftKsDef.DEF_STRATEGY_CLASS,
                replicationFactor,
                Arrays.asList(cfDef));

        // Add the schema to the cluster.
        cluster.addKeyspace(newKeyspace);

    }

    public void store(Event event) {

        SuperCfTemplate<String, String, String> template
                = new ThriftSuperCfTemplate<String, String, String>(keyspace, columnFamily,
                        SERIALIZER, SERIALIZER, SERIALIZER);

        SuperCfUpdater<String, String, String> updater = template.createUpdater(event.getSource());
        updater.addSuperColumn(event.getTimestamp().toString());
        for (Map.Entry<String, String> entry : event.getAttributes().entrySet()) {
            updater.setString(entry.getKey(), entry.getValue());
        }
        template.update(updater);

    }

    public List<Event> findEvents(String source, DateTime start, DateTime end, int max) {

        String from = (start == null) ? "" : start.toString();
        String till = (end == null) ? "" : end.toString();

        SuperSliceQuery<String, String, String, String> query =
                createSuperSliceQuery(
                        keyspace,
                        SERIALIZER,
                        SERIALIZER,
                        SERIALIZER,
                        SERIALIZER)
                        .setColumnFamily(columnFamily)
                        .setKey(source);

        if ((till.compareTo(from) > 0) || till.equals("")) {
            query.setRange(from, till, false, max);
        } else {
            // return rows in reverse order
            query.setRange(from, till, true, max);
        }

        List<HSuperColumn<String, String, String>> rows = query.execute().get().getSuperColumns();

        return Lists.transform(rows, new SuperColumnToEventConverter(source));
    }

    private class SuperColumnToEventConverter implements Function<HSuperColumn<String, String, String>, Event> {

        String source;

        SuperColumnToEventConverter(String source) {
            this.source = source;
        }

        public Event apply(@javax.annotation.Nullable HSuperColumn<String, String, String> in) {
            Map<String, String> attributes = new HashMap<String, String>();

            DateTime ts = new DateTime(in.getName());
            for (HColumn<String, String> column : in.getColumns()) {
                String key = column.getName();
                String value = column.getValue();
                attributes.put(key, value);
            }

            return Event.createEvent(ts, source, attributes);
        }

    }
}

