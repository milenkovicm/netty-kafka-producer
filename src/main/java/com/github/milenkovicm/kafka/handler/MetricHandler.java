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
package com.github.milenkovicm.kafka.handler;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.milenkovicm.kafka.ProducerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

//https://dropwizard.github.io/metrics/3.1.0/getting-started/
@ChannelHandler.Sharable
public class MetricHandler extends ChannelDuplexHandler {

    public static final MetricRegistry metricRegistry = new MetricRegistry();
    public static final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();

    static {
        jmxReporter.start();
    }

    final ProducerProperties properties;

    final String topicName;

    final Meter bytesIn;
    final Meter bytesOut;
    final Meter messageIn;
    final Meter messageOut;

    public MetricHandler(ProducerProperties properties, String topicName) {

        this.properties = properties;
        this.topicName = topicName;

        bytesIn = metricRegistry.meter(MetricRegistry.name(MetricHandler.class.getSimpleName(), this.topicName, "bytes_in"));
        bytesOut = metricRegistry.meter(MetricRegistry.name(MetricHandler.class.getSimpleName(), this.topicName, "bytes_out"));
        messageIn = metricRegistry.meter(MetricRegistry.name(MetricHandler.class.getSimpleName(), this.topicName, "messages_in"));
        messageOut = metricRegistry.meter(MetricRegistry.name(MetricHandler.class.getSimpleName(), this.topicName, "messages_out"));

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            messageIn.mark();
            bytesIn.mark(((ByteBuf) msg).readableBytes());
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            messageOut.mark();
            bytesOut.mark(((ByteBuf) msg).readableBytes());
        }

        super.write(ctx, msg, promise);
    }
}
