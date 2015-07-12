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

import com.github.milenkovicm.kafka.connection.DataKafkaBroker;
import com.github.milenkovicm.kafka.protocol.Acknowledgment;
import com.github.milenkovicm.kafka.protocol.KafkaException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;

public class DataBrokerTest extends AbstractSingleBrokerTest {

    public static final String TEST_MESSAGE = "test message from netty - netty likes kafka";
    final String topic = "test";

    @Test
    public void test_sendMessage() throws Exception {
        createTopic(topic);

        CountDownLatch latch = new CountDownLatch(1);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);

        DataKafkaBroker dataChannel = new DataKafkaBroker("localhost", START_PORT, 0, topic,new NioEventLoopGroup(), properties);
        dataChannel.connect().sync();

        dataChannel.send(freeLaterBuffer("1".getBytes()), 0, freeLaterBuffer(TEST_MESSAGE.getBytes()));

        final KafkaStream<byte[], byte[]> stream = consume(topic).get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        Assert.assertThat(new String(messages.next().message()), is(TEST_MESSAGE));

        dataChannel.disconnect();
    }

    @Test(expected = KafkaException.class)
    public void test_sendMessage_unknownTopic() throws Exception {

        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);

        DataKafkaBroker dataChannel = new DataKafkaBroker("localhost", START_PORT, 0, "unknown_topic",new NioEventLoopGroup(), properties);
        dataChannel.connect().sync();

        final ChannelFuture future = dataChannel.send(freeLaterBuffer("1".getBytes()), 0, freeLaterBuffer(TEST_MESSAGE.getBytes()));
        future.sync();

        dataChannel.disconnect();
    }

    @Test
    public void test_sendMessage_unknownTopicNoAck() throws Exception {

        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        properties.override(ProducerProperties.DATA_ACK, Acknowledgment.WAIT_FOR_NO_ONE);

        DataKafkaBroker dataChannel = new DataKafkaBroker("localhost", START_PORT, 0, "unknown_topic", new NioEventLoopGroup(), properties);
        dataChannel.connect().sync();

        final ChannelFuture future = dataChannel.send(freeLaterBuffer("1".getBytes()), 0, freeLaterBuffer(TEST_MESSAGE.getBytes()));
        future.sync();

        dataChannel.disconnect();
    }
}