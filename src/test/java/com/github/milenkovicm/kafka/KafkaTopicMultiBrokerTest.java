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
package com.github.milenkovicm.kafka;

import static org.hamcrest.CoreMatchers.is;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.Unpooled;

public class KafkaTopicMultiBrokerTest extends AbstractMultiBrokerTest {

    public static final String TEST_MESSAGE = "test message from netty - good job";

    @Test
    public void test_producer() throws Exception {

        String topic = "test";

        createTopic(topic, 4, 1);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        properties.override(ProducerProperties.PARTITIONER, new RRPartitioner());

        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 0 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 1 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 2 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 3 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));

        final List<KafkaStream<byte[], byte[]>> consume = consume(topic);

        final KafkaStream<byte[], byte[]> stream = consume.get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        List<Integer> partitions = new LinkedList<>();

        MessageAndMetadata<byte[], byte[]> message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        Assert.assertThat("partition and key should be equal", (byte) message.partition(), is(message.key()[0]));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        Assert.assertThat("partition and key should be equal", (byte) message.partition(), is(message.key()[0]));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        Assert.assertThat("partition and key should be equal", (byte) message.partition(), is(message.key()[0]));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        Assert.assertThat("partition and key should be equal", (byte) message.partition(), is(message.key()[0]));
        partitions.add(message.partition());

        Collections.sort(partitions);
        Assert.assertThat("we should have message from all partitions", partitions.toArray(), is((Object[]) new Integer[] { 0, 1, 2, 3 }));
        producer.disconnect().sync();

    }
}
