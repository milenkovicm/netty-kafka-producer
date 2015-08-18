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
import com.github.milenkovicm.kafka.handler.MetadataHandler;
import com.github.milenkovicm.kafka.handler.TerminalHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LoggingHandler;

public class ControlKafkaBroker extends AbstractKafkaBroker {

    public ControlKafkaBroker(String host, int port, String topicName, EventLoopGroup workerGroup, ProducerProperties properties) {
        super(host, port, topicName, workerGroup, properties);
    }

    @Override
    protected ChannelInitializer<SocketChannel> pipeline() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(new LengthFieldBasedFrameDecoder(Short.MAX_VALUE, 0, 4, 0, 4));
                if (properties.get(ProducerProperties.NETTY_DEBUG_PIPELINE)) {
                    pipeline.addLast(new LoggingHandler());
                }
                pipeline.addLast(new MetadataHandler(properties));
                pipeline.addLast(new TerminalHandler());
            }
        };
    }

    public KafkaPromise fetchMetadata(String topicName) {
        KafkaPromise kafkaPromise = new KafkaPromise(channel.eventLoop());
        channel.writeAndFlush(new ControlTuple(kafkaPromise, topicName));
        return kafkaPromise;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
