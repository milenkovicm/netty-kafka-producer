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

import io.netty.buffer.Unpooled;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.CoreMatchers.is;

@Ignore("test works but it may need some time to finish as kafka controller waits some time to replicate data before it dies")
public class FailureTest extends AbstractMultiBrokerTest {
    public static final String TEST_MESSAGE = "test message from netty - good job";

    @BeforeClass
    public static void start() {
        BROKER_COUNT = 2;
        AbstractMultiBrokerTest.start();
    }

    @Test
    public void test_producer() throws Exception {

        String topic = "test";

        createTopic(topic, 2, 2);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_THREAD_COUNT, 1);
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        properties.override(ProducerProperties.PARTITIONER, new RRPartitioner());

        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 0 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 1 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaServers.get(0).shutdown();
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 2 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));
        kafkaTopic.send(Unpooled.wrappedBuffer(new byte[] { 3 }), freeLaterBuffer(TEST_MESSAGE.getBytes()));

        final List<KafkaStream<byte[], byte[]>> consume = consume(topic);

        final KafkaStream<byte[], byte[]> stream = consume.get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        Set<Integer> partitions = new TreeSet<>();

        MessageAndMetadata<byte[], byte[]> message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        partitions.add(message.partition());

        message = messages.next();
        Assert.assertThat("message should be equal", new String(message.message()), is(TEST_MESSAGE));
        partitions.add(message.partition());

        Assert.assertThat("we should have message from all partitions", partitions.toArray(), is((Object[]) new Integer[] { 0, 1 }));
        producer.disconnect().sync();

    }
}
