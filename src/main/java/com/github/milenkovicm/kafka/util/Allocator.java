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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import java.lang.reflect.Field;

public class Allocator {

    private Allocator() {
    }

    public static PooledByteBufAllocator createPooledByteBufAllocator(boolean allowCache, int numCores) {
        if (numCores == 0) {
            numCores = Runtime.getRuntime().availableProcessors();
        }
        return new PooledByteBufAllocator(PlatformDependent.directBufferPreferred(), Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"),
                numCores), Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), PlatformDependent.directBufferPreferred() ? numCores : 0),
                getPrivateStaticField("DEFAULT_PAGE_SIZE"), getPrivateStaticField("DEFAULT_MAX_ORDER"),
                allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
                allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0, allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE")
                        : 0);
    }

    /** Used to get defaults from Netty's private static fields. */
    private static int getPrivateStaticField(String name) {
        try {
            Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
