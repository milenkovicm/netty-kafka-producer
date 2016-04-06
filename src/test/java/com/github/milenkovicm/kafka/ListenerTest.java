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

import com.github.milenkovicm.kafka.protocol.Acknowledgment;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;

public class ListenerTest extends AbstractSingleBrokerTest {

    public static final String TEST_MESSAGE = "test message from netty - netty likes kafka";

    @Test
    public void test_success() throws Exception {

        String topic = "test_success";

        createTopic(topic, 1);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        final CountDownLatch latch = new CountDownLatch(1);

        final Future send = kafkaTopic.send(null, freeLaterBuffer(TEST_MESSAGE.getBytes()));

        send.addListener(new FutureListener() {
            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    latch.countDown();
                }
            }
        });

        final List<KafkaStream<byte[], byte[]>> consume = consume(topic);

        final KafkaStream<byte[], byte[]> stream = consume.get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        Assert.assertThat(TEST_MESSAGE, is(new String(messages.next().message())));
        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        producer.disconnect().sync();
    }

    @Test
    public void test_producer_multi_message() throws Exception {

        String topic = "test_producer_multi_message";
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        createTopic(topic);

        final CountDownLatch latch = new CountDownLatch(3);
        FutureListener listener = new FutureListener() {
            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    latch.countDown();
                }
            }
        };

        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        kafkaTopic.send(null, freeLaterBuffer((TEST_MESSAGE + "01").getBytes())).addListener(listener);
        kafkaTopic.send(null, freeLaterBuffer((TEST_MESSAGE + "02").getBytes())).addListener(listener);
        kafkaTopic.send(null, freeLaterBuffer((TEST_MESSAGE + "03").getBytes())).addListener(listener);

        final KafkaStream<byte[], byte[]> stream = consume(topic).get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        Assert.assertThat(new String(messages.next().message()), is(TEST_MESSAGE + "01"));
        Assert.assertThat(new String(messages.next().message()), is(TEST_MESSAGE + "02"));
        Assert.assertThat(new String(messages.next().message()), is(TEST_MESSAGE + "03"));
        Assert.assertTrue("latch failed", latch.await(2, TimeUnit.SECONDS));
        producer.disconnect().sync();
    }

    @Test
    @Ignore("not sure how to test this one")
    public void test_fail() throws Exception {

        String topic = "doesnotexist";

        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        final CountDownLatch latch = new CountDownLatch(1);

        final Future<Void> send = kafkaTopic.send(null, freeLaterBuffer(TEST_MESSAGE.getBytes()));

        send.addListener(new FutureListener<Void>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess()) {
                    latch.countDown();
                }
            }
        });

        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        producer.disconnect().sync();
    }

    @Test(expected = Exception.class)
    public void test_no_acks_register_listener() throws Exception {

        String topic = "test_no_acks_register_listener";

        createTopic(topic, 1);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        properties.override(ProducerProperties.DATA_ACK, Acknowledgment.WAIT_FOR_NO_ONE);
        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        final Future send = kafkaTopic.send(null, freeLaterBuffer(TEST_MESSAGE.getBytes()));

        send.addListener(new FutureListener() {
            @Override
            public void operationComplete(Future future) throws Exception {
                Assert.fail("shouldn't get here");
            }
        });

        final List<KafkaStream<byte[], byte[]>> consume = consume(topic);

        final KafkaStream<byte[], byte[]> stream = consume.get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();

        Assert.assertThat(TEST_MESSAGE, is(new String(messages.next().message())));
        producer.disconnect().sync();
    }

    @Test
    public void test_no_acks_send_message() throws Exception {

        String topic = "test_no_acks_send_message";

        createTopic(topic, 1);
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        properties.override(ProducerProperties.DATA_ACK, Acknowledgment.WAIT_FOR_NO_ONE);
        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
        KafkaTopic kafkaTopic = producer.topic();

        kafkaTopic.send(null, freeLaterBuffer(TEST_MESSAGE.getBytes()));

        final List<KafkaStream<byte[], byte[]>> consume = consume(topic);
        final KafkaStream<byte[], byte[]> stream = consume.get(0);
        final ConsumerIterator<byte[], byte[]> messages = stream.iterator();
        Assert.assertThat(TEST_MESSAGE, is(new String(messages.next().message())));
        producer.disconnect().sync();
    }

}
