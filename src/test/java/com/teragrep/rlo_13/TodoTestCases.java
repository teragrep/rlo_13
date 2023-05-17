/*
   Java Stateful File Reader rlo_13
   Copyright (C) 2023  Suomen Kanuuna Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.teragrep.rlo_13;

public class TodoTestCases {

    public void manyFiles() {
        // for i in {1..1000}; do echo ping > ${i}; done
    }

    public void testFileChannelCache() {
        // remove, add, invalidate tests
    }

    public void testDeletedFileWhileReading () {
        // make sure state is removed
    }

    public void testTruncation() {
        // test file truncation support
    }

    public void testOverMonitoredFileConsumerBufferSizeFiles() {
        // test files over 32KB (default buffer size)
    }
    public void testOverMonitoredFileConsumerBufferSizeLines() {
        // test line over 32KB (default buffer size)
    }

    public void testStateStoreOverflow() {
        // statestore declares .setMapSize(10_485_760) this can be overflowed
    }

    public void testStateStoreVersioning () {
        // test reading statestore version
    }

    public void testWritesWithoutImmediateNewline() {
        /*
        POC done in python but should be trivial to do in java, *requires* flushing to be done after every write
        >>> fh = open("/tmp/rlo_13_test/input.log", "w+")
        >>> fh.write("hei")
        3
        >>> fh.flush()
        >>> fh.write("\n")
        1
        >>> fh.flush()
        >>> fh.write("mitäs täällä on\n")
        16
        >>> fh.flush()
        >>> fh.write("Mutta onko tätä missään?")
        24
        >>> fh.flush()
        >>> fh.write("\n")
        1
        >>> fh.flush()
        >>> fh.write("pimpeli pom\n")
        12
        >>> fh.flush()
        >>> fh.write("ping")
        4
        >>> fh.write("\n")

        Should result in ->
        Event 1: hei
        Event 2: mitäs täällä on
        Event 3: Mutta onko tätä missään?
        Event 4: pimpeli pom
        Event 5: ping
        */
    }
}
