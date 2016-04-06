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

import io.netty.util.internal.EmptyArrays;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum Error {

    //An unexpected server error
    UNKNOWN(-1),

    //No error--it worked!
    NO_ERROR(0),

    //The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
    OFFSET_OUT_OF_RANGE(1),

    //This indicates that a message contents does not match its CRC
    INVALID_MESSAGE(2),

    //This request is for a topic or partition that does not exist on this broker.
    UNKNOWN_TOPIC_OR_PARTITION(3),

    //The message has a negative size
    INVALID_MESSAGE_SIZE(4),

    //This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
    LEADER_NOT_AVAILABLE(5),

    //This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
    NOT_LEADER_FOR_PARTITION(6),

    //This error is thrown if the request exceeds the user-specified time limit in the request.
    REQUEST_TIMED_OUT(7),

    //This is not a client facing error and is used mostly by tools when a broker is not alive.
    BROKER_NOT_AVAILABLE(8),

    //If replica is expected on a broker, but is not (this can be safely ignored).
    REPLICA_NOT_AVAILABLE(9),

    //The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
    MESSAGE_SIZE_TOO_LARGE(10),

    //Internal error code for broker-to-broker communication.
    STALE_CONTROLLER_EPOCH_CODE(11),

    //If you specify a string larger than configured maximum for offset metadata
    OFFSET_METADATA_TOO_LARGE(12),

    //The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
    OFFSET_LOAD_IN_PROGRESS(14),

    //The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
    CONSUMER_COORDINATOR_NOT_AVAILABLE(15),

    //The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
    NOT_COORDINATOR_FOR_CONSUMER(16);

    private final static Map<Integer, Error> lookup;

    static {
        Map<Integer, Error> map = new HashMap<>();

        for (Error error : Error.values()) {
            map.put(error.value, error);
        }

        lookup = Collections.unmodifiableMap(map);
    }

    public final int value;
    public final KafkaException exception;

    Error(int value) {
        this.value = value;
        exception = new KafkaException(this);
        exception.setStackTrace(EmptyArrays.EMPTY_STACK_TRACE);
    }

    public static Error valueOf(int error) {
        return lookup.get(error);
    }
}
