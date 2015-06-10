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

import java.util.HashMap;
import java.util.Map;

import com.github.milenkovicm.kafka.ProducerProperties;
import com.github.milenkovicm.kafka.protocol.*;
import com.github.milenkovicm.kafka.protocol.Error;
import com.github.milenkovicm.kafka.util.PureJavaCrc32;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.GenericFutureListener;

public abstract class AbstractProducerHandler extends ChannelDuplexHandler {

    final int timeout;
    final int ack;
    // cache topic and clientid byte representation
    // no need to encode it every time
    final String clientId;
    final byte[] clientIdEncoded;
    final String topicName;
    final byte[] topicNameEncoded;

    final PureJavaCrc32 crc = new PureJavaCrc32();
    final byte[] buffer = new byte[4096];
    final AckHandle ackHandle;

    int correlationId = 0;

    Convert.DecodeItem<ProduceResponse.PartitionProduceResponse> decodePartitionProduceResponse = new Convert.DecodeItem<ProduceResponse.PartitionProduceResponse>() {
        @Override
        public ProduceResponse.PartitionProduceResponse decode(ByteBuf message) {
            return new ProduceResponse.PartitionProduceResponse(Convert.decodeInteger(message), Convert.decodeShort(message),
                    Convert.decodeLong(message));
        }
    };
    Convert.DecodeItem<ProduceResponse.TopicProduceResponse> decodeTopicProduceResponse = new Convert.DecodeItem<ProduceResponse.TopicProduceResponse>() {
        @Override
        public ProduceResponse.TopicProduceResponse decode(ByteBuf message) {
            return new ProduceResponse.TopicProduceResponse(Convert.decodeString(message), Convert.decodeGenericArray(message,
                    decodePartitionProduceResponse));
        }
    };

    public AbstractProducerHandler(String topicName, ProducerProperties properties) {
        this.clientId = properties.get(ProducerProperties.PRODUCER_ID);
        this.topicName = topicName;
        this.ack = properties.get(ProducerProperties.DATA_ACK).value;
        this.timeout = properties.get(ProducerProperties.TIMEOUT);
        this.ackHandle = properties.get(ProducerProperties.DATA_ACK) == Acknowledgment.WAIT_FOR_NO_ONE ? new NullAckHandle() : new MapAckHandle();
        this.topicNameEncoded = Convert.getBytesFromString(topicName);
        this.clientIdEncoded = Convert.getBytesFromString(clientId);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf message = (ByteBuf) msg;

        final ProduceResponse produceResponse = parseProduceResponse(message);
        if (produceResponse.topics.get(0).partitions.get(0).errorCode != 0) {
            // TODO: this should be handled better when there is more then one partition involved
            Error error = Error.valueOf(produceResponse.topics.get(0).partitions.get(0).errorCode);
            this.ackHandle.fail(produceResponse.correlationId, error.exception);
            // TODO: should we disconnect channel if there is error ... maybe disconnection may help to recover
        } else {
            this.ackHandle.success(produceResponse.correlationId);
        }

        message.release();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        final ChannelPromise channelPromise = this.ackHandle.track(ctx, correlationId, promise);
        correlationId++;
        super.write(ctx, msg, channelPromise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
        super.disconnect(ctx, future);
        this.ackHandle.destroy();
    }

    void createMessageHeader(int totalLength, ByteBuf buffer) {
        Convert.encodeInteger(totalLength, buffer);// length
        Convert.encodeShort(Api.Key.PRODUCE_REQUEST.value, buffer);//api key
        Convert.encodeShort(Api.VERSION, buffer); // api version
        Convert.encodeInteger(correlationId, buffer); // correlation id
        Convert.encodeString(clientId, buffer); //clientid
        Convert.encodeShort(ack, buffer); // ACKS ?
        Convert.encodeInteger(timeout, buffer); // timeout

        //
        // [TopicName [Partition MessageSetSize MessageSet]]
        //

        // TOPICS
        Convert.encodeInteger(1, buffer); // array length for [TopicName
        Convert.encodeString(topicNameEncoded, buffer); // topic (topic name)

        // PARTITION
        // number of partitions we're sending message to
        Convert.encodeInteger(1, buffer); // array length for [Partition MessageSetSize MessageSet]
    }

    void updateCrc(ByteBuf buffer) {
        int crc = (int) calculateCrc(buffer, 24);
        Convert.encodeInteger(crc, buffer, 20); //go back ans set CRC
    }

    long calculateCrc(ByteBuf messageSet, int position) {
        crc.reset();

        int readableBytes = messageSet.readableBytes();
        int bufferLength = buffer.length;

        while (position < readableBytes) {
            int length = readableBytes - position >= bufferLength ? bufferLength : readableBytes - position;
            messageSet.getBytes(position, buffer, 0, length);
            crc.update(buffer, 0, length);

            position += bufferLength;
        }
        return crc.getValue();
    }

    ProduceResponse parseProduceResponse(ByteBuf message) {
        return new ProduceResponse(Convert.decodeInteger(message), Convert.decodeGenericArray(message, decodeTopicProduceResponse));
    }

    interface AckHandle {
        ChannelPromise track(ChannelHandlerContext context, Integer correlationId, ChannelPromise promise);

        void fail(Integer correlationId, Throwable cause);

        void success(Integer correlationId);

        void destroy();
    }

    static class NullAckHandle implements AckHandle {

        @Override
        public ChannelPromise track(ChannelHandlerContext context, Integer correlationId, ChannelPromise promise) {
            // NOOP
            return promise;
        }

        @Override
        public void fail(Integer correlationId, Throwable cause) {
            // NOOP
        }

        @Override
        public void success(Integer correlationId) {
            // NOOP
        }

        @Override
        public void destroy() {
            // NOOP
        }
    }

    static class MapAckHandle implements AckHandle {

        private final Map<Integer, ChannelPromise> acks = new HashMap<>();

        public ChannelPromise track(final ChannelHandlerContext context, final Integer correlationId, final ChannelPromise promise) {
            // not sure if it is possible that tread may read response message before it
            // puts future in the map. that's why I have it here .
            acks.put(correlationId, promise);
            // generate new promise to track write
            // and in case of it's failure remove original promise from map
            final ChannelPromise channelPromise = context.channel().newPromise();
            channelPromise.addListener(new GenericFutureListener<ChannelPromise>() {
                @Override
                public void operationComplete(ChannelPromise future) throws Exception {
                    // shouldn't be possible to cancel this operation
                    // if it isn't success mark as failure
                    if (future.isDone() && !future.isSuccess()) {
                        fail(correlationId, future.cause());
                    }
                }
            });

            return channelPromise;
        }

        public void fail(Integer correlationId, Throwable cause) {
            ChannelPromise promise = acks.get(correlationId);
            if (promise != null) {
                acks.remove(correlationId);
                promise.setFailure(cause);
            }
        }

        public void success(Integer correlationId) {
            ChannelPromise promise = acks.get(correlationId);
            if (promise != null) {
                acks.remove(correlationId);
                promise.setSuccess();
            }
        }

        @Override
        public void destroy() {
            for (ChannelPromise promise : acks.values()) {
                promise.setFailure(Error.BROKER_NOT_AVAILABLE.exception);
            }
            acks.clear();
        }
    }
}
