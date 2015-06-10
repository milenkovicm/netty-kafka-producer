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

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import com.github.milenkovicm.kafka.protocol.Acknowledgment;
import com.github.milenkovicm.kafka.util.BackoffStrategy;
import com.github.milenkovicm.kafka.util.ParkBackoffStrategy;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

// https://kafka.apache.org/documentation.html#producerconfigs
public class ProducerProperties {

    public static final ProducerProperty<String> PRODUCER_ID = new ProducerProperty<>("default");
    public static final ProducerProperty<Integer> TIMEOUT = new ProducerProperty<>(1000);
    public static final ProducerProperty<Partitioner> PARTITIONER = new ProducerProperty<>((Partitioner) new DefaultPartitioner());
    public static final ProducerProperty<Acknowledgment> DATA_ACK = new ProducerProperty<>(Acknowledgment.WAIT_FOR_LEADER);
    public static final ProducerProperty<Integer> RETRY_BACKOFF = new ProducerProperty<>(500);
    //public static final ProducerProperty<Integer> TOPIC_METADATA_REFRESH = new ProducerProperty<>(600 * 1000);
    public static final ProducerProperty<Boolean> NETTY_DEBUG_PIPELINE = new ProducerProperty<>(Boolean.FALSE);
    public static final ProducerProperty<Integer> NETTY_HIGH_WATERMARK = new ProducerProperty<>(100_000_000);
    public static final ProducerProperty<Integer> NETTY_LOW_WATERMARK = new ProducerProperty<>(80_000_000);
    public static final ProducerProperty<Boolean> NETTY_HANDLER_COMPOSITE = new ProducerProperty<>(Boolean.FALSE);
    public static final ProducerProperty<ByteBufAllocator> NETTY_BYTEBUFF_ALLOCATOR = new ProducerProperty<>(ByteBufAllocator.DEFAULT);
    public static final ProducerProperty<BackoffStrategy> BACKOFF_STRATEGY = new ProducerProperty<>((BackoffStrategy) new ParkBackoffStrategy());

    public static final ProducerProperty<Integer> SO_TIMEOUT = new ProducerProperty<>(0);
    public static final ProducerProperty<Integer> SO_RCVBUF = new ProducerProperty<>(0);
    public static final ProducerProperty<Integer> SO_SNDBUF = new ProducerProperty<>(0);

    final ConcurrentMap<ProducerProperty, Object> properties = PlatformDependent.newConcurrentHashMap();

    public <V> void override(ProducerProperty<V> property, V value) {
        Objects.requireNonNull(property, "property");
        Objects.requireNonNull(value, "value");

        properties.put(property, value);
    }

    public <V> V get(ProducerProperty<V> property) {
        Objects.requireNonNull(property, "property");
        V v = (V) properties.get(property);

        return v == null ? property.defaultValue() : v;
    }

    static class ProducerProperty<V> {

        private final V defaultValue;

        private ProducerProperty(V defaultValue) {
            this.defaultValue = defaultValue;
        }

        public V defaultValue() {
            return defaultValue;
        }
    }
}
