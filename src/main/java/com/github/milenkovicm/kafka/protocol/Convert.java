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

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;

/**
 * @see "https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol"
 */
public class Convert {

    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static final int NULL_VALUE_LENGTH = -1;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_SHORT = 2;

    public static void encodeByte(int integer, ByteBuf buf) {
        buf.writeByte(integer);
    }

    public static void encodeShort(int integer, ByteBuf buf) {
        buf.writeShort(integer);
    }

    public static void encodeInteger(int integer, ByteBuf buf) {
        buf.writeInt(integer);
    }

    public static void encodeInteger(int integer, ByteBuf buf, int pos) {
        buf.setInt(pos, integer);
    }

    public static void encodeLong(int integer, ByteBuf buf) {
        buf.writeLong(integer);
    }

    public static void encodeBytes(ByteBuf bytes, ByteBuf buf) {
        if (bytes != null) {
            int readable = bytes.readableBytes();
            encodeInteger(readable, buf);
            buf.writeBytes(bytes, 0, readable); // content
        } else {
            encodeInteger(NULL_VALUE_LENGTH, buf); // indicates null
        }
    }

    public static int sizeOfBytes(ByteBuf bytes) {
        if (bytes != null) {
            return SIZE_OF_INT + bytes.readableBytes();
        } else {
            return SIZE_OF_INT;
        }

    }

    public static void encodeBytes(byte[] bytes, ByteBuf buf) {
        if (bytes != null) {
            int readable = bytes.length;
            encodeInteger(readable, buf); // int32 (length field)
            buf.writeBytes(bytes, 0, readable); // content
        } else {
            encodeInteger(NULL_VALUE_LENGTH, buf); // indicates null
        }
    }

    public static void encodeString(String str, ByteBuf buf) {
        if (str != null) {
            byte[] bytes = getBytesFromString(str);
            int readable = bytes.length;
            if (readable <= Short.MAX_VALUE) {
                encodeShort(readable, buf); // int16 (length field)
                buf.writeBytes(bytes, 0, readable); // content
            } else {
                throw new RuntimeException("Size of string exceeds " + Short.MAX_VALUE + ".");
            }
        } else {
            encodeShort(NULL_VALUE_LENGTH, buf); // indicates null
        }
    }

    public static byte[] getBytesFromString(String str) {
        return str.getBytes(DEFAULT_CHARSET);
    }

    public static void encodeString(byte[] str, ByteBuf buf) {
        if (str != null) {
            int readable = str.length;
            if (readable <= Short.MAX_VALUE) {
                encodeShort(readable, buf); // int16 (length field)
                buf.writeBytes(str, 0, readable); // content
            } else {
                throw new RuntimeException("Size of string exceeds " + Short.MAX_VALUE + ".");
            }
        } else {
            encodeShort(NULL_VALUE_LENGTH, buf); // indicates null
        }
    }

    public static int sizeOfString(String str) {
        if (str == null) {
            return SIZE_OF_SHORT;
        } else {
            return SIZE_OF_SHORT + getBytesFromString(str).length;
        }
    }

    public static int sizeOfString(byte[] str) {
        if (str == null) {
            return SIZE_OF_SHORT;
        } else {
            return SIZE_OF_SHORT + str.length;
        }
    }

    public static <T> void encodeGenericsArray(ByteBuf buf, EncodeItem<T> encodeItem, T... objects) {
        Convert.encodeInteger(objects.length, buf);
        for (int i = 0; i < objects.length; i++) {
            encodeItem.encode(objects[i], buf);
        }
    }

    public static void encodeStringArray(ByteBuf buf, String... strings) {
        encodeGenericsArray(buf, new EncodeItem<String>() {
            @Override
            public void encode(String string, ByteBuf buf) {
                encodeString(string, buf);
            }
        }, strings);
    }

    public static void encodeIntegerArray(ByteBuf buf, Integer... integers) {
        encodeGenericsArray(buf, new EncodeItem<Integer>() {
            @Override
            public void encode(Integer integer, ByteBuf buf) {
                encodeInteger(integer, buf);
            }
        }, integers);
    }

    public static byte decodeByte(ByteBuf buf) {
        return buf.readByte();
    }

    public static short decodeShort(ByteBuf buf) {
        return buf.readShort();
    }

    public static int decodeInteger(ByteBuf buf) {
        return buf.readInt();
    }

    public static long decodeLong(ByteBuf buf) {
        return buf.readLong();
    }

    public static List<Integer> decodeIntegerArray(ByteBuf buf) {
        return decodeGenericArray(buf, new DecodeItem<Integer>() {
            @Override
            public Integer decode(ByteBuf message) {
                return decodeInteger(message);
            }
        });
    }

    public static ByteBuf decodeBytes(ByteBuf buf) {
        int readable = decodeInteger(buf); // N => int32
        return buf.readBytes(readable); // content
    }

    public static String decodeString(ByteBuf buf) {
        int readable = decodeShort(buf); // N => int16
        ByteBuf bytes = buf.readBytes(readable); // content

        if (bytes.hasArray()) {
            return new String(bytes.array(), DEFAULT_CHARSET);
        } else {
            byte[] array = new byte[readable];
            bytes.readBytes(array);
            return new String(array, DEFAULT_CHARSET);
        }
    }

    public static <T> List<T> decodeGenericArray(ByteBuf buf, DecodeItem<T> decodeItem) {
        final List<T> result = new LinkedList<>();
        final int itemCount = decodeInteger(buf);
        for (int i = 0; i < itemCount; i++) {
            result.add(decodeItem.decode(buf));
        }
        return result;
    }

    public static abstract class DecodeItem<T> {
        public abstract T decode(ByteBuf message);
    }

    public static abstract class EncodeItem<T> {
        public abstract void encode(T object, ByteBuf buf);
    }

}
