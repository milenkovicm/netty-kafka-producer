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

public class Api {
    public static final int VERSION = 0;

    private Api() {
    }

    public enum Key {

        PRODUCE_REQUEST(0), FETCH_REQUEST(1), OFFSET_REQUEST(2), METADATA_REQUEST(3), INTERNAL_1(4), INTERNAL_2(5), INTERNAL_3(6), INTERNAL_4(7), OFFSET_COMMIT_REQUEST(
                8), OFFSET_FETCH_REQUEST(9), CONSUMER_METADATA_REQUEST(10);

        public final int value;

        Key(int value) {
            this.value = value;
        }
    }
}
