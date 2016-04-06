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

import com.github.milenkovicm.kafka.protocol.Error;
import com.github.milenkovicm.kafka.protocol.KafkaException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class NoTopicTest extends AbstractSingleBrokerTest {

    @Test(expected = KafkaException.class)
    public void test_producer_no_topic_sync() throws Exception {

        String topic = "test_producer_no_topic__sync";
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        //createTopic(topic);

        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        producer.connect().sync();
    }

    @Test
    public void test_producer_no_topic_async() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        String topic = "test_producer_no_topic__async";
        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);
        //createTopic(topic);

        KafkaProducer producer = new KafkaProducer("localhost", START_PORT, topic, properties);
        final Future<Void> connect = producer.connect();

        connect.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                latch.countDown();
            }
        });

        latch.await(5, TimeUnit.SECONDS);

        Assert.assertThat(connect.isDone(), is(true));
        Assert.assertThat(connect.isSuccess(), is(false));
        Assert.assertThat(connect.cause(), notNullValue());
        Assert.assertThat(((KafkaException) connect.cause()).error, is (Error.LEADER_NOT_AVAILABLE));

        producer.disconnect();
    }
}

