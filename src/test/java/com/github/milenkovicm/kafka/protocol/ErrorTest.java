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

import static org.hamcrest.CoreMatchers.is;

import org.junit.Assert;
import org.junit.Test;

public class ErrorTest {

    @Test(expected = KafkaException.class)
    public void test_error() {
        throw Error.BROKER_NOT_AVAILABLE.exception;
    }

    @Test
    public void test_error_value() {
        ErrorThrower errorThrower = new ErrorThrower();
        try {
            new ErrorThrower().makeSomeError();
        } catch (KafkaException exception) {
            Assert.assertThat(exception.getStackTrace().length, is(0));
            Assert.assertThat(exception.error, is(errorThrower.error));
            return;
        }
        Assert.fail("shouldn't get here");

    }

    static class ErrorThrower {
        final Error error = Error.BROKER_NOT_AVAILABLE;

        public void makeSomeError() {
            throw error.exception;
        }
    }
}
