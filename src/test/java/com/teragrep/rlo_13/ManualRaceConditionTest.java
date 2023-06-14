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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class ManualRaceConditionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManualRaceConditionTest.class);
    @Test
    @EnabledIfSystemProperty(named = "runManualRaceConditionTest", matches = "true")
    public void manualTmpTest() throws InterruptedException, IOException {

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
        };

        Thread writer = new Thread(() -> {
            LOGGER.info("Starting writer thread");
            try {
                Path path = Paths.get("/tmp/rlo_13_race/log");
                Files.createDirectories(path);
                FileWriter fileWriter = new FileWriter(path + "/input.txt");
                for(int i=0; i<Integer.MAX_VALUE; i++) {
                    fileWriter.write(testMessage);
                    fileWriter.flush();
                    if(i%100000==0) {
                        fileWriter.close();
                        Files.delete(Paths.get(path + "/input.txt"));
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

        Thread truncator = new Thread(() -> {
            LOGGER.info("Starting truncator thread");
            try {
                Path path = Paths.get("/tmp/rlo_13_race/log/input.txt");
                while(true) {
                    try (FileChannel fc = FileChannel.open(path, WRITE)) {
                        fc.truncate(0);
                    }
                    Thread.sleep(1000);
                }
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }
        });
        truncator.start();

        /*
        Files.createDirectories(Paths.get("/tmp/rlo_13_race/state"));
        try (StatefulFileReader statefulFileReader =
                     new StatefulFileReader(
                             Paths.get("/tmp/rlo_13_race/state"),
                             consumerSupplier
                     )
        ) {
            DirectoryEventWatcher dew = new DirectoryEventWatcher(
                    Paths.get("/tmp/rlo_13_race/log"),
                    false,
                    Pattern.compile("^.*$"),
                    statefulFileReader
            );

            dew.watch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }*/
        while (true) {
            readFile();
        }
    }

    private void readFile() {
        Path path = Paths.get("/tmp/rlo_13_race/log/input.txt");
        FileRecord fileRecord = new FileRecord(path);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32*1024);
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(1024*1024); // todo configurable


        // !! trace log existing position


        try (FileChannel fileChannel = FileChannel.open(path, READ)) {
            fileChannel.position(0);

            long bytesRead = 0;
            while (fileChannel.position() < fileChannel.size()) {
                bytesRead = fileChannel.read(byteBuffer);

                if (bytesRead  < 1) {
                    return;
                }

                byteBuffer.flip(); // reading
                while (byteBuffer.hasRemaining()) {
                    byte b = byteBuffer.get();

                    if (b == '\0') {
                        throw new RuntimeException("nullia ja monta " + bytesRead);
                    }

                    if (b != '\n' && outputBuffer.position() != outputBuffer.capacity() - 1) {
                        outputBuffer.put(b);
                    }
                    else {
                        outputBuffer.put((byte) '\n');
                        outputBuffer.flip();
                        LOGGER.trace("Produced fileRecord at <{}>", fileChannel.position());

                        byte[] bytes = new byte[outputBuffer.remaining()];
                        outputBuffer.get(bytes);

                        fileRecord.setEndOffset(fileChannel.position());
                        fileRecord.setRecord(bytes);
                        //LOGGER.info("bytesRead <{}>", bytesRead);

                        // for next one
                        fileRecord.setStartOffset(fileChannel.position()); // next if any
                        fileRecord.setRecord(new byte[0]);
                        outputBuffer.clear();
                    }
                    bytesRead--;
                }
                byteBuffer.clear();
            }
            // persistence at lastRecordStart, partial ones will be re-read
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }
}
