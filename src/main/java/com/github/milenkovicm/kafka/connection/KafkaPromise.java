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

package com.github.milenkovicm.kafka.connection;

import com.github.milenkovicm.kafka.protocol.MetadataResponse;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;

//                                              +---------------------------+
//                                              | Completed successfully    |
//                                              +---------------------------+
//                                         +---->      isDone() = true      |
//         +--------------------------+    |    |   isSuccess() = true      |
//         |        Uncompleted       |    |    +===========================+
//         +--------------------------+    |    | Completed with failure    |
//         |      isDone() = false    |    |    +---------------------------+
//         |   isSuccess() = false    |----+---->   isDone() = true         |
//         | isCancelled() = false    |    |    |    cause() = non-null     |
//         |    cause() = null        |    |    +===========================+
//         +--------------------------+    |    | Completed by cancellation |
//                                         |    +---------------------------+
//                                         +---->      isDone() = true      |
//                                              | isCancelled() = true      |
//                                              +---------------------------+

public class KafkaPromise extends DefaultPromise<MetadataResponse> {

    KafkaPromise(EventExecutor executor) {
        super(executor);
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("cancel");
    }
}
