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

package com.github.milenkovicm.kafka.example;

import com.github.milenkovicm.kafka.KafkaProducer;
import com.github.milenkovicm.kafka.KafkaTopic;
import com.github.milenkovicm.kafka.ProducerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class Example1 {

    public static void main(String[] args) throws InterruptedException {

        ProducerProperties properties = new ProducerProperties();
        properties.override(ProducerProperties.NETTY_DEBUG_PIPELINE, true);

        ByteBuf key = ByteBufAllocator.DEFAULT.buffer(10); // add some key here
        ByteBuf value = ByteBufAllocator.DEFAULT.buffer(10); // and value here

        KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties);
        producer.connect().sync();

        KafkaTopic kafkaTopic = producer.topic();
        kafkaTopic.send(key,value);

        producer.disconnect().sync();
    }
}
