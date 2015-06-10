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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.kafka.AbstractTest;
import com.github.milenkovicm.kafka.ProducerProperties;
import com.github.milenkovicm.kafka.protocol.ProduceResponse;

public class ProduceResponseParseTest extends AbstractTest {

    final static byte[] PRODUCE_RESPONSE_1 = { 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

    //                     +-------------------------------------------------+
    //                     |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
    //            +--------+-------------------------------------------------+----------------+
    //            |00000000| 00 00 00 01 00 00 00 01 00 04 74 65 73 74 00 00 |..........test..|
    //            |00000010| 00 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 |................|
    //            +--------+-------------------------------------------------+----------------+

    final CopyProducerHandler handler = new CopyProducerHandler("test", new ProducerProperties());

    @Test
    public void test_produce_response_parsing() {
        ProduceResponse produce = handler.parseProduceResponse(freeLaterBuffer(PRODUCE_RESPONSE_1));
        Assert.assertThat(produce, is(notNullValue()));
        Assert.assertThat(produce.topics.size(), is(1));
        ProduceResponse.TopicProduceResponse topic = produce.topics.get(0);
        Assert.assertThat(topic.topicName, is("test"));
        Assert.assertThat(topic.partitions.size(), is(1));
        ProduceResponse.PartitionProduceResponse partition = topic.partitions.get(0);
        Assert.assertThat(partition.errorCode, is((short) 0));
        Assert.assertThat(partition.partition, is(0));
        Assert.assertThat(partition.offset, is(0L));
    }
}
