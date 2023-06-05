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

import com.teragrep.rlo_13.statestore.InMemoryStateStore;
import com.teragrep.rlo_13.statestore.StateStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class MonitoredFileConsumerTest {
    private void createTestFile(Path filePath, int records, boolean append) throws IOException {
        try (FileWriter fileWriter = new FileWriter(filePath.toFile(), append)) {
            for (int i = 0; i < records; i++) {
                fileWriter.write(i+"\n");
                fileWriter.flush();
            }
        }
    }
    @Test
    public void testReadFile() throws IOException {
        AtomicLong recordCounter = new AtomicLong();
        Path testFilePath = Paths.get("target/MonitoredFileConsumerTest#createTestFile");

        int records = 10;

        try (FileChannelCache fcc = new FileChannelCache()) {

            Consumer<FileRecord> frc = fileRecord -> recordCounter.incrementAndGet();

            StateStore stateStore = new InMemoryStateStore();

            MonitoredFileConsumer mfc = new MonitoredFileConsumer(fcc, stateStore, frc);

            createTestFile(testFilePath, records, false);
            mfc.readFile(testFilePath);
        }

        Assertions.assertEquals(records, recordCounter.get());
    }

    @Test
    public void testReadAppendedFile() throws IOException {
        AtomicLong recordCounter = new AtomicLong();
        Path testFilePath = Paths.get("target/MonitoredFileConsumerTest#testReadAppendedFile");

        int records = 10;

        try (FileChannelCache fcc = new FileChannelCache()) {

            Consumer<FileRecord> frc = fileRecord -> recordCounter.incrementAndGet();

            StateStore stateStore = new InMemoryStateStore();

            MonitoredFileConsumer mfc = new MonitoredFileConsumer(fcc, stateStore, frc);

            // create
            createTestFile(testFilePath, records, false);

            // read 1
            mfc.readFile(testFilePath);

            // append
            createTestFile(testFilePath, records, true);

            // read 2
            mfc.readFile(testFilePath);
        }

        Assertions.assertEquals(records * 2, recordCounter.get());
    }

    @Test
    public void testReadDeletedFile() throws IOException {
        AtomicLong recordCounter = new AtomicLong();
        Path testFilePath = Paths.get("target/MonitoredFileConsumerTest#testReadDeletedFile");

        int records = 10;

        try (FileChannelCache fcc = new FileChannelCache()) {

            Consumer<FileRecord> frc = fileRecord -> recordCounter.incrementAndGet();

            StateStore stateStore = new InMemoryStateStore();

            MonitoredFileConsumer mfc = new MonitoredFileConsumer(fcc, stateStore, frc);

            // create
            try (FileWriter fileWriter = new FileWriter(testFilePath.toFile(), false)) {
                // write 1
                for (int i = 0; i < records; i++) {
                    fileWriter.write(i+"\n");
                    fileWriter.flush();
                }

                // read 1
                mfc.readFile(testFilePath);

                // delete
                Files.delete(testFilePath);

                // write 2
                for (int i = 0; i < records; i++) {
                    fileWriter.write(i+"\n");
                    fileWriter.flush();
                }

                // read 2
                mfc.readFile(testFilePath);
            }
        }

        Assertions.assertEquals(records * 2, recordCounter.get());
    }
}
