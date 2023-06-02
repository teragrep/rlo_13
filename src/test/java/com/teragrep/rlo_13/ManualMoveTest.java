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

import com.teragrep.rlo_12.DirectoryEventWatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class ManualMoveTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManualMoveTest.class);
    @Test
    @EnabledIfSystemProperty(named = "runManualMoveTest", matches = "true")
    public void manualTmpTest() throws InterruptedException, IOException {
        AtomicLong readCounter = new AtomicLong();
        AtomicLong writeCounter = new AtomicLong();

        String testMessage = "START" +  new String(new char[1000]).replace("\0", "X") + "END\n";
        int messageSize = testMessage.length();

        Supplier<Consumer<FileRecord>> consumerSupplier = () -> fileRecord -> {
            if(fileRecord.getRecord().length != messageSize) {
                throw new RuntimeException(
                        "Got an unexpected message: <["
                                + new String(fileRecord.getRecord(), StandardCharsets.UTF_8)
                                + "]>, size "
                                + fileRecord.getRecord().length
                                + " so <"+fileRecord.getStartOffset()+">"
                                + " eo <"+fileRecord.getEndOffset()+">"
                );
            }
            else {
                readCounter.incrementAndGet();
            }
        };

        Thread writer = new Thread(() -> {
            LOGGER.info("Starting writer thread");
            try {
                Path path = Paths.get("/tmp/rlo_13_move/log");
                Files.createDirectories(path);
                FileWriter fileWriter = new FileWriter(path + "/input.txt");
                for(int i=0; i<Integer.MAX_VALUE; i++) {
                    try {
                        Thread.sleep(1);
                    }
                    catch (InterruptedException ignored) {

                    }

                    fileWriter.write(testMessage);
                    fileWriter.flush();
                    writeCounter.incrementAndGet();
                    if(i%100000==0) {
                        fileWriter.close();
                        try {
                            Files.delete(Paths.get(path + "/input.txt"));
                        }
                        catch (Exception ignored) {

                        }
                        fileWriter = new FileWriter(path + "/input.txt");
                        i=0;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writer.start();
        Thread.sleep(1000);

        Thread mover = new Thread(() -> {
            LOGGER.info("Starting move thread");
            try {
                Path path1 = Paths.get("/tmp/rlo_13_move/log/input.txt");
                Path path2 = Paths.get("/tmp/rlo_13_move/log/input.txt.1");
                while(true) {
                    try {
                        Thread.sleep(1000);
                        Files.move(path1, path2, REPLACE_EXISTING);
                        LOGGER.warn("written <{}> read <{}>", writeCounter.get(), readCounter.get());
                    }
                    catch (Exception ignored) {

                    }
                }
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }
        });
        mover.start();

        Files.createDirectories(Paths.get("/tmp/rlo_13_move/state"));
        try (StatefulFileReader statefulFileReader =
                     new StatefulFileReader(
                             Paths.get("/tmp/rlo_13_move/state"),
                             consumerSupplier
                     )
        ) {
            DirectoryEventWatcher dew = new DirectoryEventWatcher(
                    Paths.get("/tmp/rlo_13_move/log"),
                    false,
                    Pattern.compile("^.*txt$"),
                    statefulFileReader
            );

            dew.watch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
