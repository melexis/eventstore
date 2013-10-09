/**
 * Eventstore implementation in Cassandra.
 *
 * This implementation uses multiple column families that are manually populated as indexes.
 * The dataschema consists out of the following column families:
 *
 * - EVENT
 *   A unique event with as rowkey a compound of source and uuid for the event,  and as columns the fields in the event.
 * - EVENTS_PER_PROCESSID
 *   An index on EVENT with as rowkey the source and the process id,  and as columns the event UUID
 * - PROCS_PER_LOTNAME
 *   An index on EVENTS_PER_PROCESSID with as rowkey the source and the lotname.
 *   The column names are a compound of the ProcessId, a time UUID and the fieldname. ( Fieldnames consist out of "LAST_EVENT_NAME" and "TIMESTAMP" )
 *   The column values contain the value for the field.
 * - TIMESTAMP_CLUSTER
 *   A sorted set of timestamp clustered together.
 *   The row key is a compound of the source and the index name for which the timestamps are clustered,
 *   the columns contain as key the timestamp of the last entry and as value the uuid of the cluster.
 * - EVENTS_PER_TIMESTAMP
 *   An index on EVENT with as rowkey timestamp cluster uuid,  and as column names the timestamps.
 *   The values contains the event uuid.
 */

package com.melexis.esb.eventstore.impl;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.melexis.common.Tuple;
import com.melexis.esb.eventstore.Event;
import com.melexis.iterators.PropertyLists;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.Assert;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.*;

import static org.antlr.tool.ErrorManager.assertTrue;

public class EventDaoCassandraWithCFSImpl implements EventDao {

    private final static String PROCS_PER_LOTNAME = "PROCESSES_PER_LOTNAME";
    private final static String EVENTS_PER_PROCESSID = "EVENTS_PER_PROCESSID";
    private final static String EVENTS_PER_TIMESTAMP = "EVENTS_PER_TIMESTAMP";
    private final static String TIMESTAMP_CLUSTER = "TIMESTAMP_CLUSTER";
    private final static String EVENTS = "EVENT";

    private final Keyspace keyspace;

    public EventDaoCassandraWithCFSImpl(final Cluster cluster,
                                        final String keyspace,
                                        final String strategy,
                                        int replicationFactor) {

        final KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace);
        // TODO: migrate the old schema to the new schema
        if (keyspaceDef == null) {
            createSchema(cluster, keyspace, strategy, replicationFactor);
        }

        this.keyspace = HFactory.createKeyspace(keyspace, cluster);
    }

    private void createSchema(final Cluster cluster,
                              final String keyspace,
                              final String strategy,
                              int replicationFactor) {
        final List<ColumnFamilyDefinition> cfdefs = Lists.transform(
                Arrays.asList(EVENTS, EVENTS_PER_TIMESTAMP, EVENTS_PER_PROCESSID, PROCS_PER_LOTNAME, TIMESTAMP_CLUSTER),
                new Function<String, ColumnFamilyDefinition>() {
                    @Override
                    public ColumnFamilyDefinition apply(@Nullable String cf) {
                        final ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
                                keyspace,
                                cf,
                                ComparatorType.UTF8TYPE);

                        cfDef.setColumnType(ColumnType.STANDARD);
                        cfDef.setKeyValidationClass("org.apache.cassandra.db.marshal.UTF8Type");
                        cfDef.setDefaultValidationClass("org.apache.cassandra.db.marshal.UTF8Type");
                        return cfDef;
                    }
                });

        final KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(
                keyspace,
                strategy,
                replicationFactor,
                cfdefs);

        cluster.addKeyspace(ksDef);
    }

    @Override
    public void store(Event event) {
        throw new AssertionError("not implemented");
    }

    @Override
    public List<Event> findEvents(final String source,
                                  final @Nullable DateTime from,
                                  final @Nullable DateTime till,
                                  final int max) {
        Assert.isTrue(max > 0, "Max needs to be larger than 0");
        int n = 0;
        final List<Event> events = new ArrayList<>(max);
        final String start = from == null ? null : from.toString();
        final String finish = till == null ? null : till.toString();

        final List<HColumn<String, String>> clusterColumns = findTimeSeriesCluster(source, start, finish);

        for (final HColumn<String, String> clusterColumn: clusterColumns) {
            final List<HColumn<String, String>> eventsIdColumns =
                    findEventsPerTimestamp(max, n, start, finish, clusterColumn.getValue());
            n += eventsIdColumns.size();

            for (HColumn<String, String> eventIdColumn: eventsIdColumns) {
                final Event event = findEventWithId(source, eventIdColumn.getValue());
                events.add(event);

            }

            if (n >= max) {
                break;
            }
        }

        return events;
    }

    private Event findEventWithId(final String source,
                                  final String id) {
        final SliceQuery<String, String, String> esq = new ThriftSliceQuery<>(
                keyspace,
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get());

        esq.setKey(id);
        esq.setRange(" ", "~", false, 100);
        esq.setColumnFamily(EVENTS);


        DateTime dt = null;
        final ImmutableMap.Builder<String, String> propsB = new ImmutableMap.Builder<>();

        for (final HColumn<String, String> col: esq.execute().get().getColumns()) {
            if (col.getName().equals("DATE")) {
                dt = new DateTime(col.getValue());
            } else {
                propsB.put(col.getName(), col.getValue());
            }
        }

        final ImmutableMap<String, String> properties = propsB.build();
        return new Event(dt, source, properties);
    }

    private List<HColumn<String, String>> findEventsPerTimestamp(int max,
                                                                 int n,
                                                                 String start,
                                                                 String finish,
                                                                 final String clusterId) {
        final SliceQuery<String, String, String> tsSq = new ThriftSliceQuery<>(
                keyspace,
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get());

        // the cluster uuid
        tsSq.setKey(clusterId);
        tsSq.setRange(start, finish, false, max - n);
        tsSq.setColumnFamily(EVENTS_PER_TIMESTAMP);

        return tsSq.execute().get().getColumns();
    }

    private List<HColumn<String, String>> findTimeSeriesCluster(String source, String start, String finish) {
        final SliceQuery<String, String, String> csq = new ThriftSliceQuery<>(
                keyspace,
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get());

        csq.setKey(String.format("%s/%s", source, EVENTS_PER_TIMESTAMP));
        csq.setRange(
                start,
                finish,
                false, 5);

        csq.setColumnFamily(TIMESTAMP_CLUSTER);
        return csq.execute().get().getColumns();
    }

    @Override
    public List<Event> findEventsForLotnameAndSource(final String lotname,
                                                     final String source,
                                                     @Nullable final DateTime from,
                                                     @Nullable final DateTime till,
                                                     final int max) {
        // search for the newest
    }

    @Override
    public List<Event> findEventsForProcessIdAndSource(String processId, String source, @Nullable DateTime start, @Nullable DateTime end, int max) {
        throw new AssertionError("not implemented");
    }
}
