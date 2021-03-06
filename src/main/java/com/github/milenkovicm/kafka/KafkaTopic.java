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

import com.github.milenkovicm.kafka.connection.AbstractKafkaBroker;
import com.github.milenkovicm.kafka.connection.DataKafkaBroker;
import com.github.milenkovicm.kafka.protocol.Acknowledgment;
import com.github.milenkovicm.kafka.util.BackoffStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.Arrays;

public class KafkaTopic {

    final Partitioner partitioner;
    final ByteBufAllocator allocator;
    final BackoffStrategy backoffStrategy;
    final Acknowledgment ack;
    final String topicName;

    private volatile DataKafkaBroker[] partitions = new DataKafkaBroker[0];

    KafkaTopic(Partitioner partitioner, ProducerProperties properties, String topicName) {
        this.partitioner = partitioner;
        this.topicName = topicName;
        this.allocator = properties.get(ProducerProperties.NETTY_BYTE_BUF_ALLOCATOR);
        this.backoffStrategy = properties.get(ProducerProperties.BACKOFF_STRATEGY);
        this.ack = properties.get(ProducerProperties.DATA_ACK);
    }

    synchronized void initialize(int numberOfPartitions) {
        if (this.partitions.length != 0) {
            return;
        }
        this.partitions = new DataKafkaBroker[numberOfPartitions];
    }

    synchronized void set(DataKafkaBroker dataKafkaChannel, int partition) {
        final DataKafkaBroker[] partitions = this.partitions;
        if (partition < 0 || partition >= this.partitions.length) {
            throw new RuntimeException("no such partition: " + partition);
        }
        if (partitions[partition] != null && partitions[partition].equals(dataKafkaChannel)) {
            return;
        }

        final DataKafkaBroker[] dataKafkaChannels = Arrays.copyOf(partitions, partitions.length);
        dataKafkaChannels[partition] = dataKafkaChannel;

        this.partitions = dataKafkaChannels;
    }

    DataKafkaBroker get() {
        final DataKafkaBroker[] partitions = this.partitions;
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] != null) {
                return partitions[i];
            }
        }
        return null;
    }

    synchronized void remove(DataKafkaBroker dataKafkaChannel) {
        final DataKafkaBroker[] partitions = this.partitions;
        final DataKafkaBroker[] dataKafkaChannels = Arrays.copyOf(partitions, partitions.length);

        for (int i = 0; i < partitions.length; i++) {
            if (dataKafkaChannels[i] != null && dataKafkaChannels[i].equals(dataKafkaChannel)) {
                dataKafkaChannels[i] = null;
            }
        }
        this.partitions = dataKafkaChannels;
    }

    public int numberOfPartitions() {
        DataKafkaBroker[] partitions = this.partitions;
        return (partitions == null) ? 0 : partitions.length;
    }

    public String topicName(){
        return topicName;
    }

    public Future<Void> send(ByteBuf key, ByteBuf message) {
        int partition = partitioner.partition(key, partitions.length);
        return send(key, partition, message);
    }

    public Future<Void> send(ByteBuf key, int partitionId, ByteBuf message) {

        if (partitionId < 0 || partitionId >= this.partitions.length) {
            throw new RuntimeException("no such partition: " + partitionId);
        }

        AbstractKafkaBroker partition = this.partitions[partitionId];

        if (partition == null) {
            this.release(key, message);
            return this.getDefaultChannelPromise();
        }

        final Channel channel = partition.channel();
        final ChannelPromise channelPromise = this.getChannelPromise(channel);

        if (!channel.isWritable()) {
            if (backoffStrategy.handle(channel, key, message)) {
                channelPromise.cancel(true);
                return channelPromise;
            }
        }

        final ByteBuf messageSet = DataKafkaBroker.createMessageSet(allocator, key, partitionId, message);
        this.release(key, message);

        return channel.writeAndFlush(messageSet, channelPromise);
    }

    private ChannelPromise getChannelPromise(Channel channel) {
        return ack == Acknowledgment.WAIT_FOR_NO_ONE ? channel.voidPromise() : channel.newPromise();
    }

    private DefaultChannelPromise getDefaultChannelPromise() {
        final DefaultChannelPromise channelPromise = new DefaultChannelPromise(null, GlobalEventExecutor.INSTANCE);
        channelPromise.cancel(true);
        return channelPromise;
    }

    private void release(ByteBuf key, ByteBuf message) {
        if (message != null) {
            message.release();
        }

        if (key != null) {
            key.release();
        }
    }


    @Override
    public String toString() {
        return "KafkaTopic{" +
                "partitions=" + Arrays.toString(partitions) +
                "partitionNum=" + this.numberOfPartitions() +
                ", topicName='" + topicName + '\'' +
                ", partitioner=" + partitioner +
                ", ack=" + ack +
                '}';
    }
}
