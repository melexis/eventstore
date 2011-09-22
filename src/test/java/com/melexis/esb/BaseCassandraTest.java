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

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.testutils.EmbeddedServerHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BaseCassandraTest {
    private static Logger log = LoggerFactory.getLogger(BaseCassandraTest.class);

    private static EmbeddedServerHelper embedded;
    public static final StringSerializer STRING_SERIALIZER = StringSerializer.get();

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws org.apache.thrift.transport.TTransportException
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws org.apache.cassandra.config.ConfigurationException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
        log.info("in setup of BaseEmbedded.Test");
        embedded = new EmbeddedServerHelper();
        embedded.setup();
    }

    @AfterClass
    public static void teardown() throws IOException {
        EmbeddedServerHelper.teardown();
        embedded = null;
    }
}
