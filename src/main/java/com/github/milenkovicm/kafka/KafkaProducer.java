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

import com.github.milenkovicm.kafka.connection.ControlKafkaBroker;
import com.github.milenkovicm.kafka.connection.DataKafkaBroker;
import com.github.milenkovicm.kafka.connection.KafkaPromise;
import com.github.milenkovicm.kafka.protocol.MetadataResponse;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    final EventExecutor eventExecutor;
    final Promise<Void> connectPromise;
    final Promise<Void> disconnectPromise;

    final String hostname;
    final int port;
    final String topicName;

    final KafkaTopic kafkaTopic;
    final ProducerProperties properties;
    final NioEventLoopGroup workerGroup;
    final Map<Integer, DataKafkaBroker> brokers = new ConcurrentHashMap<>();

    volatile ControlKafkaBroker control = null;
    volatile int numberOfPartitions;
    volatile boolean shutdown = false;

    public KafkaProducer(String hostname, int port, String topicName, ProducerProperties properties, NioEventLoopGroup workerGroup) {
        this.properties = properties;
        this.hostname = hostname;
        this.port = port;
        this.topicName = topicName;
        this.workerGroup = workerGroup;

        this.kafkaTopic = new KafkaTopic(properties.get(ProducerProperties.PARTITIONER), properties);

        this.eventExecutor = GlobalEventExecutor.INSTANCE;
        this.connectPromise = new DefaultPromise<>(eventExecutor);
        this.disconnectPromise = new DefaultPromise<>(eventExecutor);

    }

    public KafkaProducer(String hostname, int port, String topicName, ProducerProperties properties) {
        this(hostname,port,topicName,properties,new NioEventLoopGroup(properties.get(ProducerProperties.NETTY_THREAD_COUNT),
                new DefaultThreadFactory("producer-"+topicName,Thread.MAX_PRIORITY)));
    }

    public KafkaProducer(String hostname, int port, String topicName) {
        this(hostname, port, topicName, new ProducerProperties());
    }

    public KafkaTopic topic() {
        return kafkaTopic;
    }

    public Future<Void> connect() {
        this.connectControl(hostname, port, connectPromise);
        return connectPromise;
    }

    public Future<Void> disconnect() {
        this.shutdown = true;

        List<Future<?>> futures = new ArrayList<>();

        final ChannelFuture disconnectControl = control.channel().disconnect();
        futures.add(disconnectControl);
        for (DataKafkaBroker data : brokers.values()) {
            final Future<?> disconnectData = data.disconnect();
            futures.add(disconnectData);
        }
        brokers.clear();
        control = null;

        if (futures.size() > 0) {
            final PromiseAggregator<Void, ChannelFuture> promiseAggregator = new PromiseAggregator<Void, ChannelFuture>(disconnectPromise);
            promiseAggregator.add(futures.toArray(new Promise[1]));
        } else {
            disconnectPromise.setSuccess(null);
        }
        this.workerGroup.shutdownGracefully();
        this.eventExecutor.shutdownGracefully();
        return disconnectPromise;
    }

    // connect control channel which handles all metadata queries
    void connectControl(String hostname, int port, Promise<Void> promise) {
        ControlKafkaBroker control = new ControlKafkaBroker(hostname, port, topicName, workerGroup, properties);
        this.control = control;
        control.connect().addListener(new ControlConnectListener(promise));
        control.channel().closeFuture().addListener(new ControlDisconnectedListener(control));
    }

    void connectData(MetadataResponse metadataResponse, Promise<Void> promise) {
        LOGGER.debug("create data connections ...");

        MetadataResponse.TopicMetadata topic = metadataResponse.topics.get(0);
        this.numberOfPartitions = topic.partitions.size();
        this.kafkaTopic.initialize(this.numberOfPartitions);

        this.assignPartitions(metadataResponse, promise);
        LOGGER.debug("init done!");
    }

    void assignPartitions(MetadataResponse metadataResponse, Promise<Void> promise) {
        LOGGER.debug("assign partitions {}", metadataResponse);
        MetadataResponse.TopicMetadata topic = metadataResponse.topics.get(0);

        List<ChannelFuture> promises = new LinkedList<>();

        for (MetadataResponse.PartitionMetadata partition : topic.partitions) {
            LOGGER.debug("brokerId: [{}] assigned to partitionId: [{}]", partition.leader, partition.partitionId);
            DataKafkaBroker dataKafkaChannel = getDataChannel(metadataResponse, partition.leader, promises);
            this.kafkaTopic.set(dataKafkaChannel, partition.partitionId);
        }
        completePromise(promise, promises);
    }

    void completePromise(final Promise<Void> promise, List<ChannelFuture> promises) {
        if (promises.size() > 0) {
            final PromiseAggregator<Void, ChannelFuture> promiseAggregator = new PromiseAggregator<Void, ChannelFuture>(promise) {
            };
            promiseAggregator.add(promises.toArray(new Promise[1]));
        } else {
            promise.setSuccess(null);
        }
    }

    DataKafkaBroker getDataChannel(MetadataResponse metadataResponse, Integer brokerId, List<ChannelFuture> promises) {
        DataKafkaBroker dataKafkaChannel = brokers.get(brokerId);

        if (dataKafkaChannel != null) {
            return dataKafkaChannel;
        } else {
            final MetadataResponse.Broker broker = findBroker(metadataResponse, brokerId);
            final DataKafkaBroker channel = connectDataChannel(promises, broker);
            brokers.put(broker.nodeId, channel);

            return channel;
        }
    }

    DataKafkaBroker connectDataChannel(List<ChannelFuture> promises, MetadataResponse.Broker broker) {
        LOGGER.debug("connecting data channel to broker:[{}] hostname: [{}] port:[{}]", broker.nodeId, broker.host, broker.port);
        final DataKafkaBroker channel = new DataKafkaBroker(broker.host, broker.port, broker.nodeId, topicName, workerGroup, properties);

        final ChannelFuture future = channel.connect();
        future.channel().closeFuture().addListener(new BrokerDisconnectedListener(channel));
        promises.add(future);
        return channel;
    }

    MetadataResponse.Broker findBroker(MetadataResponse metadataResponse, Integer brokerId) {
        for (MetadataResponse.Broker broker : metadataResponse.brokers) {
            if (broker.nodeId == brokerId) {
                return broker;
            }
        }
        throw new ProducerException("cant find broker with id: " + brokerId);
    }

    class ControlDisconnectedListener implements ChannelFutureListener {
        final ControlKafkaBroker controlKafkaChannel;

        ControlDisconnectedListener(ControlKafkaBroker controlKafkaChannel) {
            this.controlKafkaChannel = controlKafkaChannel;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (shutdown) {
                return;
            }
            LOGGER.warn("control channel: [{}] disconnected! ", future.channel());
            control = null;
            controlKafkaChannel.disconnect();
            eventExecutor.schedule(new UpdateControlChannel(), properties.get(ProducerProperties.RETRY_BACKOFF), TimeUnit.MILLISECONDS);
        }
    }

    class ControlConnectListener implements ChannelFutureListener {
        final Promise<Void> promise;

        ControlConnectListener(Promise<Void> promise) {
            this.promise = promise;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                //
                // couldn't connect control channel
                promise.setFailure(future.cause());
                LOGGER.debug("control connection failed! hostname:[{}] port:[{}]", hostname, port);
            } else {
                LOGGER.debug("control connection connected! hostname:[{}] port:[{}]", hostname, port);
                // control channel is connected
                // fetch metadata
                final KafkaPromise kafkaPromise = control.fetchMetadata(topicName);
                kafkaPromise.addListener(new FutureListener<MetadataResponse>() {
                    @Override
                    public void operationComplete(Future<MetadataResponse> future) throws Exception {
                        if (future.isSuccess()) {
                            // metadata fetched
                            // future will succeed when all data channels connect
                            connectData(future.get(), promise);
                        } else {
                            // error fetching metadata
                            promise.setFailure(future.cause());
                        }
                    }
                });
            }
        }
    }

    class BrokerDisconnectedListener implements ChannelFutureListener {

        final DataKafkaBroker dataKafkaChannel;

        BrokerDisconnectedListener(DataKafkaBroker dataKafkaChannel) {
            this.dataKafkaChannel = dataKafkaChannel;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (shutdown) {
                return;
            }
            LOGGER.warn("brokerId: [{}] disconnected! ", dataKafkaChannel);
            kafkaTopic.remove(dataKafkaChannel);
            brokers.remove(dataKafkaChannel.brokerId);
            dataKafkaChannel.disconnect();
            eventExecutor.schedule(new UpdateDataChannels(), properties.get(ProducerProperties.RETRY_BACKOFF), TimeUnit.MILLISECONDS);
        }
    }

    class UpdateDataChannels implements Runnable {

        @Override
        public void run() {
            ControlKafkaBroker control = KafkaProducer.this.control;
            if (control == null) {
                return;
            }
            control.fetchMetadata(topicName).addListener(new GenericFutureListener<Future<MetadataResponse>>() {
                @Override
                public void operationComplete(Future<MetadataResponse> future) throws Exception {
                    connectData(future.get(), new DefaultPromise<Void>(eventExecutor));
                }
            });
            LOGGER.debug("update data connection ... done");
        }
    }

    class UpdateControlChannel implements Runnable {

        @Override
        public void run() {
            final DataKafkaBroker dataKafkaChannel = kafkaTopic.get();
            final DefaultPromise<Void> promise = new DefaultPromise<Void>(eventExecutor);
            if (dataKafkaChannel != null) {
                connectControl(dataKafkaChannel.hostname, dataKafkaChannel.port, promise);
                LOGGER.debug("update control connection from data connection values... done");
            } else {
                connectControl(hostname, port, promise);
                LOGGER.debug("update control connection from default values ... done");
            }
        }
    }
}

