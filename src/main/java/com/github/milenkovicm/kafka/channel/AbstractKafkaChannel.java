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
package com.github.milenkovicm.kafka.channel;

import com.github.milenkovicm.kafka.ProducerProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKafkaChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaChannel.class);
    public final String hostname;
    public final int port;
    final Bootstrap bootstrap;
    final EventLoopGroup workerGroup;
    final String topicName;
    final ProducerProperties properties;

    protected volatile Channel channel;

    public AbstractKafkaChannel(String hostname, int port, String topicName, EventLoopGroup workerGroup, ProducerProperties properties) {
        this.hostname = hostname;
        this.port = port;
        this.topicName = topicName;
        this.workerGroup = workerGroup;
        this.properties = properties;

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(this.workerGroup);
        this.bootstrap.channel(NioSocketChannel.class);
        this.bootstrap.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, properties.get(ProducerProperties.NETTY_HIGH_WATERMARK));
        this.bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, properties.get(ProducerProperties.NETTY_LOW_WATERMARK));

        this.bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        if (properties.get(ProducerProperties.SO_TIMEOUT) > 0) {
            this.bootstrap.option(ChannelOption.SO_TIMEOUT, 0);
        }

        if (properties.get(ProducerProperties.SO_RCVBUF) > 0) {
            this.bootstrap.option(ChannelOption.SO_RCVBUF, 0);
        }

        if (properties.get(ProducerProperties.SO_SNDBUF) > 0) {
            this.bootstrap.option(ChannelOption.SO_SNDBUF, 0);
        }

        this.bootstrap.handler(pipeline());
    }

    abstract protected ChannelInitializer<SocketChannel> pipeline();

    public Channel channel() {
        return channel;
    }

    public ChannelFuture connect() {
        final ChannelFuture channelFuture = bootstrap.connect(hostname, port);
        channel = channelFuture.channel();
        return channelFuture;
    }

    public ChannelFuture disconnect() {
        return channel.disconnect();
    }
}
