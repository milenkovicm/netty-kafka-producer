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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import scala.Option;

import java.io.File;
import java.util.*;

public abstract class AbstractMultiBrokerTest extends AbstractTest {

    protected static final int TIMEOUT = 10000;
    protected static final int START_BROKER_ID = 0;
    protected static final int START_PORT = 20000;//TestUtils.RandomPort();
    protected static final int DEFAULT_BROKER_COUNT = 1;
    protected static final int ZK_PORT = 2181;
    protected static final String ZK_HOST = "localhost:"+ZK_PORT;
    protected static final int ZK_CLIENT_TIMEOUT = 60000;

    protected volatile static int BROKER_COUNT = DEFAULT_BROKER_COUNT;

    protected static ZkServer zkServer;
    protected static ZkClient zkClient;
    protected static ZkUtils zkUtils;

    protected static List<KafkaServer> kafkaServers;
    protected static File zkData;
    protected static File zkLogs;
    protected static final IDefaultNameSpace DEFAULT_NAME_SPACE = new IDefaultNameSpace() {
        @Override
        public void createDefaultNameSpace(ZkClient zkClient) {

        }
    };

    @BeforeClass
    public static void start() {

        zkData = TestUtils.tempDir();
        zkLogs = TestUtils.tempDir();

        zkServer = new ZkServer(zkData.getAbsolutePath(),zkLogs.getAbsolutePath(), DEFAULT_NAME_SPACE, ZK_PORT);
        LOGGER.debug("starting zk server ...");
        zkServer.start();
        LOGGER.debug("started zk server");

        // does not work for some reason
        // zkClient = zkServer.getZkClient();
        zkClient = new ZkClient(ZK_HOST, ZK_CLIENT_TIMEOUT, ZK_CLIENT_TIMEOUT, ZKStringSerializer$.MODULE$);
        zkUtils =  ZkUtils.apply(zkClient, false);

        kafkaServers = new ArrayList<>();
        for (int i = 0; i < BROKER_COUNT; i++) {
            LOGGER.info("starting test broker id: [{}] at port: [{}]", START_BROKER_ID + i, START_PORT + i);

            Properties properties = TestUtils.createBrokerConfig(START_BROKER_ID + i,ZK_HOST,
                    true,
                    true,
                    START_PORT + i,
                    Option.apply(SecurityProtocol.PLAINTEXT),
                    Option.<File>empty(),
                    Option.<Properties>empty(),
                    true,
                    false,
                    0,
                    false,
                    0,
                    false,
                    0,
                    Option.<String>empty()
            );

            kafkaServers.add(TestUtils.createServer(new KafkaConfig(properties), new MockTime()));
        }
    }

    @AfterClass
    public static void stop() {
        for (KafkaServer kafkaServer : kafkaServers) {
            kafkaServer.shutdown();
        }

        kafkaServers.clear();
        zkClient.close();
        zkServer.shutdown();

        Utils.delete(zkData);
        Utils.delete(zkLogs);
    }

    /**
     * Creates {@code topic} on currently running broker
     *
     * @param topic
     *        name to create
     */
    public static void createTopic(String topic) {

        createTopic(topic, 1, 1);
    }

    public static void createTopic(String topic, Integer partitionNum) {
        createTopic(topic, partitionNum, 1);
    }

    public static void createTopic(String topic, Integer partitionNum, Integer replicas) {
        TestUtils.createTopic(
                        zkUtils,
                        topic,
                        partitionNum,
                        replicas,
                        scala.collection.JavaConversions.asScalaBuffer(kafkaServers),
                        new Properties()
        );
    }

    public List<KafkaStream<byte[], byte[]>> consume(String topic) {

        Properties consumerProperties = TestUtils.createConsumerProperties(
                ZK_HOST,
                UUID.randomUUID().toString(),
                "client",
                TIMEOUT);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1); // not sure why is this 1

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                Consumer.createJavaConsumerConnector (new ConsumerConfig(consumerProperties)).createMessageStreams(topicCountMap);

        return consumerMap.get(topic);
    }
}
