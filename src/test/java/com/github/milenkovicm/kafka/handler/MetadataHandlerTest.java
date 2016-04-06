/*
 * Copyright 2015 Marko Milenkovic (http://github.com/milenkovicm)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.milenkovicm.kafka.handler;

import com.github.milenkovicm.kafka.AbstractTest;
import com.github.milenkovicm.kafka.ProducerProperties;
import com.github.milenkovicm.kafka.protocol.MetadataResponse;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class MetadataHandlerTest extends AbstractTest {

    //                    +-------------------------------------------------+
    //                    |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
    //            +--------+-------------------------------------------------+----------------+
    //            |00000000| 00 00 00 00 00 00 00 01 00 00 00 00 00 09 6c 6f |..............lo|
    //            |00000010| 63 61 6c 68 6f 73 74 00 00 8f 5e 00 00 00 01 00 |calhost...^.....|
    //            |00000020| 00 00 04 74 65 73 74 00 00 00 01 00 00 00 00 00 |...test.........|
    //            |00000030| 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 |................|
    //            |00000040| 01 00 00 00 00                                  |.....           |
    //            +--------+-------------------------------------------------+----------------+

    final static byte[] METADATA_RESPONSE_1 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x6c, 0x6f, 0x63,
            0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x00, 0x00, (byte) 0xb7, (byte) 0xd1, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x74, 0x65,
            0x73, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 };

    final MetadataHandler handler = new MetadataHandler(new ProducerProperties());

    @Test
    public void test_metadata_response_parsing() {
        MetadataResponse metadata = handler.parseMetadataResponse(freeLaterBuffer(METADATA_RESPONSE_1));

        Assert.assertThat(metadata, is(notNullValue()));
        Assert.assertThat(metadata.brokers.size(), is(1));
        MetadataResponse.Broker broker = metadata.brokers.get(0);
        Assert.assertThat(broker.nodeId, is(0));
        Assert.assertThat(broker.host, is("localhost"));
        Assert.assertThat(broker.port, is(47057));

        Assert.assertThat(metadata.topics.size(), is(1));
        MetadataResponse.TopicMetadata topic = metadata.topics.get(0);
        Assert.assertThat(topic.topicErrorCode, is((short) 0));
        Assert.assertThat(topic.topicName, is("test"));
        Assert.assertThat(topic.partitions.size(), is(1));
        MetadataResponse.PartitionMetadata partition = topic.partitions.get(0);

        Assert.assertThat(partition.partitionErrorCode, is((short) 0));
        Assert.assertThat(partition.partitionId, is(0));
        Assert.assertThat(partition.leader, is(0));
        Assert.assertThat(partition.replicas.size(), is(1));
        Assert.assertThat(partition.replicas.get(0), is(0));
        Assert.assertThat(partition.isr.size(), is(1));
        Assert.assertThat(partition.isr.get(0), is(0));

    }

}
