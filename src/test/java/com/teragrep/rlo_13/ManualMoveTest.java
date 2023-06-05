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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public class ManualMoveTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManualMoveTest.class);
    @Test
    @EnabledIfSystemProperty(named = "runManualMoveTest", matches = "true")
    public void manualTmpTest() throws InterruptedException, IOException {
        AtomicLong readCounter = new AtomicLong();
        AtomicLong readA = new AtomicLong();
        AtomicLong readB = new AtomicLong();
        AtomicLong readC = new AtomicLong();
        AtomicLong readD = new AtomicLong();
        AtomicLong readE = new AtomicLong();
        AtomicLong readF = new AtomicLong();
        AtomicLong readG = new AtomicLong();
        AtomicLong readH = new AtomicLong();

        AtomicLong writeCounter = new AtomicLong();

        final String[] testMessage =
                {
                        "START" +  new String(new char[1000]).replace("\0", "A") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "B") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "C") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "D") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "E") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "F") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "G") + "END\n",
                        "START" +  new String(new char[1000]).replace("\0", "H") + "END\n"
                };
        final int messageSize = testMessage[0].length();

        final Supplier<Consumer<FileRecord>> consumerSupplier = () -> fileRecord -> {
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
                if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[0])) {
                    readA.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[1])) {
                    readB.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[2])) {
                    readC.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[3])) {
                    readD.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[4])) {
                    readE.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[5])) {
                    readF.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[6])) {
                    readG.incrementAndGet();
                }
                else if (new String(fileRecord.getRecord(), StandardCharsets.US_ASCII).equals(testMessage[7])) {
                    readH.incrementAndGet();
                }

                readCounter.incrementAndGet();
            }
        };

        Thread writer = new Thread(() -> {
            LOGGER.info("Starting writer thread");
            try {
                Path path = Paths.get("/tmp/rlo_13_move/log");
                Files.createDirectories(path);
                FileWriter fileWriter = new FileWriter(path + "/input.txt");

                int file = 0;

                for(int i=0; i<Integer.MAX_VALUE; i++) {
                    /*
                    try {
                        Thread.sleep(1);
                    }
                    catch (InterruptedException ignored) {

                    }

                     */



                    if(i == 100000) {
                        fileWriter.close();

                        file++;
                        if (file == 8) {
                            LOGGER.info("breaking writer, leaving last file here");
                            break;
                        }

                        try {
                            Thread.sleep(1000);
                            Files.delete(Paths.get(path + "/input.txt"));
                        }
                        catch (Exception ignored) {

                        }
                        fileWriter = new FileWriter(path + "/input.txt");

                        i=0;
                    }

                    fileWriter.write(testMessage[file]);
                    fileWriter.flush();
                    writeCounter.incrementAndGet();
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
                    LOGGER.warn("written <{}> read <{}>, A <{}>, B <{}>, C <{}>, D <{}>, E <{}>, F <{}>, G <{}>, H <{}>",
                            writeCounter.get(),
                            readCounter.get(),
                            readA.get(),
                            readB.get(),
                            readC.get(),
                            readD.get(),
                            readE.get(),
                            readF.get(),
                            readG.get(),
                            readH.get()
                    );
                    try {
                        Thread.sleep(1000);
                        Files.move(path1, path2, REPLACE_EXISTING);

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
