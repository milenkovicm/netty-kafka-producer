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

import java.util.ArrayDeque;
import java.util.Queue;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class AbstractTest {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTest.class);
    protected static final ByteBufAllocator DEFAULT_ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;
    private static final Queue<ByteBuf> freeLaterQueue = new ArrayDeque<>();

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param initialCapacity
     *        buffer capacity
     * @return allocated byte buffer
     */
    public static ByteBuf freeLaterBuffer(final int initialCapacity) {
        return freeLaterBuffer(DEFAULT_ALLOCATOR, initialCapacity);
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param buffer
     *        to copy
     * @return allocated byte buffer
     */
    public static ByteBuf freeLaterBuffer(final byte[] buffer) {
        return freeLaterBuffer(DEFAULT_ALLOCATOR, buffer.length).writeBytes(buffer);
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param allocator
     *        allocator to use
     * @param initialCapacity
     *        buffer capacity
     * @return allocated buffer
     */
    public static ByteBuf freeLaterBuffer(final ByteBufAllocator allocator, final int initialCapacity) {
        final ByteBuf buffer = allocator.buffer(initialCapacity);
        freeLaterQueue.add(buffer);
        return buffer;
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param buffer
     *        buffer to copy
     * @return allocated buffer
     */
    public static ByteBuf freeLaterBuffer(final ByteBuf buffer) {
        freeLaterQueue.add(buffer);
        return buffer;
    }

    @After
    public void after() {
        for (;;) {
            final ByteBuf buf = freeLaterQueue.poll();
            if (buf == null) {
                break;
            }
            if (buf.refCnt() > 0) {
                buf.release(buf.refCnt());
            }
        }
    }
}
