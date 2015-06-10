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

public class ProduceResponse extends Response {

    public final List<TopicProduceResponse> topics;

    public ProduceResponse(int correlationId, List<TopicProduceResponse> topics) {
        super(correlationId);
        this.topics = topics;
    }

    @Override
    public String toString() {
        return "ProduceResponse{" + "topics=" + topics + '}';
    }

    public static class TopicProduceResponse {

        public final String topicName;
        public final List<PartitionProduceResponse> partitions;

        public TopicProduceResponse(String topicName, List<PartitionProduceResponse> partitions) {
            this.topicName = topicName;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "TopicProduceResponse{" + "topicName='" + topicName + '\'' + ", partitions=" + partitions + '}';
        }
    }

    public static class PartitionProduceResponse {

        public final int partition;
        public final short errorCode;
        public final long offset;

        public PartitionProduceResponse(int partition, short errorCode, long offset) {
            this.partition = partition;
            this.errorCode = errorCode;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "PartitionProduceResponse{" + "partition=" + partition + ", errorCode=" + Error.valueOf(errorCode) + ", offset=" + offset + '}';
        }
    }
}
