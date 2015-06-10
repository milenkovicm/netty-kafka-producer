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

import com.github.milenkovicm.kafka.ProducerProperties;
import com.github.milenkovicm.kafka.protocol.Convert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class CopyProducerHandler extends AbstractProducerHandler {

    public CopyProducerHandler(String topicName, ProducerProperties properties) {
        super(topicName, properties);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        final ByteBuf message = (ByteBuf) msg;
        final ByteBuf kafkaMessage = creteProduceRequest(ctx.alloc(), message, topicName);
        message.release();
        super.write(ctx, kafkaMessage, promise);
    }

    ByteBuf creteProduceRequest(ByteBufAllocator allocator, ByteBuf messageSet, String topic) {
        final int messageSetSize = messageSet.readableBytes();
        // a bit hardcoded logic follows
        // total length is length of the all fields
        // i've pre-calculated sizes of arrays and strings in case of
        // topic and client id
        final int totalLength = 22 + messageSetSize + Convert.sizeOfString(topicNameEncoded) + Convert.sizeOfString(clientIdEncoded);

        ByteBuf buffer = allocator.buffer(totalLength + 4); // msg length + size of length field
        createMessageHeader(totalLength, buffer);
        updateCrc(messageSet);
        buffer.writeBytes(messageSet);

        return buffer;
    }

}
