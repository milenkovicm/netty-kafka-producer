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

import java.util.Random;
import java.util.zip.CRC32;

import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.kafka.util.PureJavaCrc32;
import io.netty.buffer.ByteBuf;

public class CrcTest extends AbstractTest {

    final CRC32 crc = new CRC32();
    final PureJavaCrc32 jcrc = new PureJavaCrc32();
    byte[] buffer = new byte[4096];

    long calculateCrc(ByteBuf messageSet) {
        crc.reset();
        for (int i = 0; i < messageSet.readableBytes(); i++) {
            crc.update(messageSet.getByte(i));
        }
        return crc.getValue();
    }

    long calculateJCrc(ByteBuf messageSet) {
        jcrc.reset();

        int readableBytes = messageSet.readableBytes();
        int bufferLength = buffer.length;
        int position = 0;

        while (position < readableBytes) {
            int length = readableBytes - position >= bufferLength ? bufferLength : readableBytes - position;
            messageSet.getBytes(position, buffer, 0, length);
            jcrc.update(buffer, 0, length);

            position += bufferLength;
        }
        return jcrc.getValue();
    }

    @Test
    public void test_crc0() {
        ByteBuf buf = getByteBuf(3561);
        Assert.assertNotEquals(0, calculateCrc(buf));
        Assert.assertNotEquals(0, calculateJCrc(buf));
        Assert.assertNotEquals(0, calculateJCrc(buf, 0));

    }

    @Test
    public void test_crc1() {
        ByteBuf buf = getByteBuf(3560);
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf));
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf, 0));
    }

    @Test
    public void test_crc2() {
        ByteBuf buf = getByteBuf(7541);
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf));
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf, 0));
    }

    @Test
    public void test_crc3() {
        ByteBuf buf = getByteBuf(4096);
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf));
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf, 0));
    }

    @Test
    public void test_crc4() {
        ByteBuf buf = getByteBuf(4095);
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf));
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf, 0));
    }

    @Test
    public void test_crc5() {
        ByteBuf buf = getByteBuf(4097);
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf));
        Assert.assertEquals(calculateCrc(buf), calculateJCrc(buf, 0));
    }

    @Test
    public void test_crc6() {
        ByteBuf buf = getByteBuf(4090);
        long crc = calculateCrc(buf);
        ByteBuf buf1 = getByteBuf(18);
        buf1.writeBytes(buf);
        System.out.println(crc);
        Assert.assertEquals(crc, calculateJCrc(buf1, 18));
    }

    long calculateJCrc(ByteBuf messageSet, int position) {
        crc.reset();

        int readableBytes = messageSet.readableBytes();
        int bufferLength = buffer.length;
        //int position = 0;

        while (position < readableBytes) {
            int length = readableBytes - position >= bufferLength ? bufferLength : readableBytes - position;
            messageSet.getBytes(position, buffer, 0, length);
            crc.update(buffer, 0, length);

            position += bufferLength;
        }
        return crc.getValue();
    }

    private ByteBuf getByteBuf(int size) {
        Random rand = new Random();
        ByteBuf buf = freeLaterBuffer(size);
        for (int i = 0; i < size; i++) {
            buf.writeByte(rand.nextInt());
        }
        //        byte[] b = new byte[size];
        //        buf.getBytes(0,b);
        //        System.out.println(Arrays.toString(b));
        return buf;
    }
}
