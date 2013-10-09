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
import com.google.common.collect.Lists;
import com.melexis.esb.eventstore.Event;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public List<Event> findEvents(String source, @Nullable DateTime from, @Nullable DateTime till, int max) {
        Assert.isTrue(max > 0);
        int n = 0;
        final List<Event> events = new ArrayList<Event>(max);
        final String start = from == null ? null : from.toString();
        final String finish = till == null ? null : till.toString();

        final SliceQuery<String, String, String> csq = new ThriftSliceQuery<String, String, String>(
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
        final List<HColumn<String, String>> clusterColumns = csq.execute().get().getColumns();

        for (final HColumn<String, String> clusterColumn: clusterColumns) {
            final SliceQuery<String, String, String> tsSq = new ThriftSliceQuery<String, String, String>(
                    keyspace,
                    StringSerializer.get(),
                    StringSerializer.get(),
                    StringSerializer.get());

            // the cluster uuid
            tsSq.setKey(clusterColumn.getValue());
            tsSq.setRange(start, finish, false, max - n);
            tsSq.setColumnFamily(EVENTS_PER_TIMESTAMP);

            final List<HColumn<String, String>> eventsIdColumns = tsSq.execute().get().getColumns();
            n += eventsIdColumns.size();

            for (HColumn<String, String> eventIdColumn: )

            if (n >= max) {
                break;
            }
        }
    }

    @Override
    public List<Event> findEventsForLotnameAndSource(String lotname, String source, @Nullable DateTime from, @Nullable DateTime till, int max) {
        throw new AssertionError("not implemented");
    }

    @Override
    public List<Event> findEventsForProcessIdAndSource(String processId, String source, @Nullable DateTime start, @Nullable DateTime end, int max) {
        throw new AssertionError("not implemented");
    }
}
