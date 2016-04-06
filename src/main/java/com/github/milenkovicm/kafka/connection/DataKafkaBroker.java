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

package com.github.milenkovicm.kafka.connection;

import com.github.milenkovicm.kafka.ProducerProperties;
import com.github.milenkovicm.kafka.handler.CompositeProducerHandler;
import com.github.milenkovicm.kafka.handler.CopyProducerHandler;
import com.github.milenkovicm.kafka.handler.MetricHandler;
import com.github.milenkovicm.kafka.handler.TerminalHandler;
import com.github.milenkovicm.kafka.protocol.Convert;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LoggingHandler;

public class DataKafkaBroker extends AbstractKafkaBroker {

    public final String clientId;
    public final int brokerId;

    final MetricHandler metricHandler;

    public DataKafkaBroker(String host, int port, int brokerId, String topicName, EventLoopGroup workerGroup, ProducerProperties properties) {
        super(host, port, topicName, workerGroup, properties);
        this.brokerId = brokerId;
        this.clientId = properties.get(ProducerProperties.PRODUCER_ID);
        this.metricHandler = new MetricHandler(properties, topicName);
    }

    public static ByteBuf createMessageSet(ByteBufAllocator allocator, ByteBuf key, int partition, ByteBuf message) {

        // MESSAGE SET SIZE
        //
        //  message
        //  key
        // --
        // message+key
        final int messageKeySize = Convert.sizeOfBytes(message) + Convert.sizeOfBytes(key); //+2 for magic byte + attributes

        // TOTAL SIZE OF PARTITION MESSAGE
        // ALLOCATE
        //
        // 4 - partition
        // 4 - length
        // 18 - message set size
        // --
        // 26
        ByteBuf buffer = allocator.buffer(26 + messageKeySize);
        Convert.encodeInteger(partition, buffer); // partition id

        // MESSAGE SET SIZE
        //
        //  8 - offset
        //  4 - msg size
        //  4 - crc
        //  1 - magic byte
        //  1 - attributes
        // --
        // 18
        Convert.encodeInteger(messageKeySize + 18, buffer); // message set size
        Convert.encodeLong(0, buffer); // offset (MAY BE ZERO ALL THE TIMES FOR PRODUCER)
        Convert.encodeInteger(messageKeySize + 6, buffer); // message size ( +4 crc size)
        Convert.encodeInteger(0, buffer); //CRC (will be calculated later)
        Convert.encodeByte(0, buffer); // magic bytes
        Convert.encodeByte(0, buffer); // attributes
        Convert.encodeBytes(key, buffer);
        Convert.encodeBytes(message, buffer);

        return buffer;
    }

    @Override
    protected ChannelInitializer<SocketChannel> pipeline() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(new LengthFieldBasedFrameDecoder(Short.MAX_VALUE, 0, 4, 0, 4));
                pipeline.addLast(metricHandler);

                if (properties.get(ProducerProperties.NETTY_DEBUG_PIPELINE)) {
                    pipeline.addLast(new LoggingHandler());
                }

                if (properties.get(ProducerProperties.NETTY_HANDLER_COMPOSITE)) {
                    pipeline.addLast(new CompositeProducerHandler(topicName, properties));
                } else {
                    pipeline.addLast(new CopyProducerHandler(topicName, properties));
                }
                pipeline.addLast(new TerminalHandler());
            }
        };
    }

    public ChannelFuture send(ByteBuf key, int partition, ByteBuf message, ChannelPromise promise) {
        final ByteBuf messageSet = createMessageSet(channel.alloc(), key, partition, message);
        return channel.writeAndFlush(messageSet, promise);
    }

    public ChannelFuture send(ByteBuf key, int partition, ByteBuf message) {
        final ByteBuf messageSet = createMessageSet(channel.alloc(), key, partition, message);
        return channel.writeAndFlush(messageSet);
    }

    @Override
    public String toString() {
        return "DataKafkaBroker{" +
                "brokerId=" + brokerId +" "+ super.toString() +
                '}';
    }
}
