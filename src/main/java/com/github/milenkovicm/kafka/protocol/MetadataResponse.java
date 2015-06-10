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
package com.github.milenkovicm.kafka.protocol;

import java.util.List;

public class MetadataResponse extends Response {

    public final List<Broker> brokers;
    public final List<TopicMetadata> topics;

    public MetadataResponse(int correlationId, List<Broker> brokers, List<TopicMetadata> topics) {
        super(correlationId);
        this.brokers = brokers;
        this.topics = topics;
    }

    @Override
    public String toString() {
        return "MetadataResponse{" + "brokers=" + brokers + ", topics=" + topics + '}';
    }

    public static class Broker {

        public final int nodeId;
        public final String host;
        public final int port;

        public Broker(int nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "Broker{" + "nodeId=" + nodeId + ", host='" + host + '\'' + ", port=" + port + '}';
        }
    }

    public static class TopicMetadata {

        public final short topicErrorCode;
        public final String topicName;
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(short topicErrorCode, String topicName, List<PartitionMetadata> partitions) {
            this.topicErrorCode = topicErrorCode;
            this.topicName = topicName;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "TopicMetadata{" + "topicErrorCode=" + Error.valueOf(topicErrorCode) + ", topicName='" + topicName + '\'' + ", partitions="
                    + partitions + '}';
        }
    }

    public static class PartitionMetadata {

        public final short partitionErrorCode;
        public final int partitionId;
        public final int leader;
        public final List<Integer> replicas;
        public final List<Integer> isr;

        public PartitionMetadata(short partitionErrorCode, int partitionId, int leader, List<Integer> replicas, List<Integer> isr) {
            this.partitionErrorCode = partitionErrorCode;
            this.partitionId = partitionId;
            this.leader = leader;
            this.replicas = replicas;
            this.isr = isr;
        }

        @Override
        public String toString() {
            return "PartitionMetadata{" + "partitionErrorCode=" + partitionErrorCode + ", partitionId=" + partitionId + ", leader=" + leader
                    + ", replicas=" + replicas + ", isr=" + isr + '}';
        }
    }
}
