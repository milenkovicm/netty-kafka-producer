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
import com.github.milenkovicm.kafka.connection.ControlTuple;
import com.github.milenkovicm.kafka.connection.KafkaPromise;
import com.github.milenkovicm.kafka.protocol.Api;
import com.github.milenkovicm.kafka.protocol.Convert;
import com.github.milenkovicm.kafka.protocol.Error;
import com.github.milenkovicm.kafka.protocol.MetadataResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MetadataHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataHandler.class);
    final String clientId;
    final byte[] clientIdEncoded;
    final Map<Integer, KafkaPromise> acks = new HashMap<>();
    protected int correlation = 0;
    Throwable throwable = new Throwable();

    Convert.DecodeItem<MetadataResponse.Broker> decodeBrokerMetadata = new Convert.DecodeItem<MetadataResponse.Broker>() {
        @Override
        public MetadataResponse.Broker decode(ByteBuf message) {
            return new MetadataResponse.Broker(Convert.decodeInteger(message), Convert.decodeString(message), Convert.decodeInteger(message));
        }
    };
    Convert.DecodeItem<MetadataResponse.PartitionMetadata> decodePartitionMetadata = new Convert.DecodeItem<MetadataResponse.PartitionMetadata>() {
        @Override
        public MetadataResponse.PartitionMetadata decode(ByteBuf message) {
            return new MetadataResponse.PartitionMetadata(Convert.decodeShort(message), Convert.decodeInteger(message),
                    Convert.decodeInteger(message), Convert.decodeIntegerArray(message), Convert.decodeIntegerArray(message));
        }
    };
    Convert.DecodeItem<MetadataResponse.TopicMetadata> decodeTopicMetadata = new Convert.DecodeItem<MetadataResponse.TopicMetadata>() {
        @Override
        public MetadataResponse.TopicMetadata decode(ByteBuf message) {
            return new MetadataResponse.TopicMetadata(Convert.decodeShort(message), Convert.decodeString(message), Convert.decodeGenericArray(
                    message, decodePartitionMetadata));
        }
    };

    public MetadataHandler(ProducerProperties properties) {
        this.clientId = properties.get(ProducerProperties.PRODUCER_ID);
        this.clientIdEncoded = Convert.getBytesFromString(clientId);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        LOGGER.debug("channel active: [{}]", ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf message = (ByteBuf) msg;

        final MetadataResponse metadataResponse = parseMetadataResponse(message);
        message.release();
        LOGGER.debug("received metadata response from broker! response: [{}]", metadataResponse);
        KafkaPromise promise = acks.get(metadataResponse.correlationId);

        if (promise != null) {
            acks.remove(metadataResponse.correlationId);

            if (metadataResponse.topics.size() > 0) {
                if (metadataResponse.topics.get(0).topicErrorCode != Error.NO_ERROR.value) {
                    final Error error = Error.valueOf(metadataResponse.topics.get(0).topicErrorCode);
                    promise.setFailure(error.exception);
                } else {
                    promise.setSuccess(metadataResponse);
                }
            }

        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        final ControlTuple tuple = (ControlTuple) msg;
        final int currentCorrelation = correlation++;
        ByteBuf kafkaMessage = createMetadataRequest(ctx.alloc(), tuple.topicName, currentCorrelation);

        // not sure if it is possible that tread may read response message before it
        // puts future in the map. that's why I have it here .
        acks.put(currentCorrelation, tuple.promise);
        promise.addListener(new GenericFutureListener<ChannelPromise>() {
            @Override
            public void operationComplete(ChannelPromise future) throws Exception {
                // shouldn't be possible to cancel this operation
                //                if (future.isCancelled()) {
                //                    tuple.promise.cancel(true);
                //                    return;
                //                }
                if (future.isDone() && !future.isSuccess()) {
                    acks.remove(currentCorrelation);
                    tuple.promise.setFailure(future.cause());
                }
            }
        });
        super.write(ctx, kafkaMessage, promise);
    }

    ByteBuf createMetadataRequest(ByteBufAllocator allocator, String topic, int correlation) {
        final int totalLength = 12 + Convert.sizeOfString(topic) + Convert.sizeOfString(clientId);
        // +4 as total message length does not include length field
        ByteBuf buffer = allocator.buffer(totalLength + 4);
        Convert.encodeInteger(totalLength, buffer);// length
        Convert.encodeShort(Api.Key.METADATA_REQUEST.value, buffer);//api key
        Convert.encodeShort(Api.VERSION, buffer); // api version
        Convert.encodeInteger(correlation, buffer); // correlation id
        Convert.encodeString(clientIdEncoded, buffer); //clientid
        Convert.encodeStringArray(buffer, topic);

        return buffer;
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
        super.disconnect(ctx, future);

        for (KafkaPromise promise : acks.values()) {
            promise.setFailure(Error.BROKER_NOT_AVAILABLE.exception);
        }
        acks.clear();
    }

    public MetadataResponse parseMetadataResponse(ByteBuf message) {
        return new MetadataResponse(Convert.decodeInteger(message), Convert.decodeGenericArray(message, decodeBrokerMetadata),
                Convert.decodeGenericArray(message, decodeTopicMetadata));
    }
}
