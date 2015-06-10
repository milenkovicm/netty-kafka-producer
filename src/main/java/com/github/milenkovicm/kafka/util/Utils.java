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
package com.github.milenkovicm.kafka.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Utils {
    private Utils() {
    }

    /**
     * Returns the next wait interval, in milliseconds, using an exponential backoff algorithm.
     *
     * @param retryCount
     *        retry count
     * @return time to wait
     */
    public static long getWaitTimeExp(int retryCount) {

        long waitTime = ((long) Math.pow(2, retryCount) * 100L);
        long maxWaitTime = 10000;
        return Math.min(waitTime, maxWaitTime);
    }

    public static List<MetadataBroker> splitBrokers(String addresses) {
        Objects.requireNonNull(addresses, "addresses");
        String[] brokers = addresses.split(",");
        List<MetadataBroker> result = new LinkedList<>();
        for (String broker : brokers) {
            String[] pair = broker.split(":");
            if (pair.length == 2) {
                // handle all possible cases some day
                result.add(new MetadataBroker(pair[0].trim(), Integer.parseInt(pair[1].trim())));
            }
        }
        return result;
    }

    public static class MetadataBroker {
        public final String host;
        public final int port;

        MetadataBroker(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "MetadataBroker{" + "host='" + host + '\'' + ", port=" + port + '}';
        }
    }
}
