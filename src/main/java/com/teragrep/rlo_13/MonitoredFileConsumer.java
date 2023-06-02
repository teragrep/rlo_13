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

import com.teragrep.rlo_12.MonitoredFile;
import com.teragrep.rlo_13.statestore.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.function.Consumer;

class MonitoredFileConsumer implements Consumer<MonitoredFile> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoredFileConsumer.class);

    private final FileChannelCache fileChannelCache;
    private final StateStore stateStore;

    private final Consumer<FileRecord> fileRecordConsumer;

    public MonitoredFileConsumer(FileChannelCache fileChannelCache, StateStore stateStore, Consumer<FileRecord> fileRecordConsumer) {
        this.fileChannelCache = fileChannelCache;
        this.stateStore = stateStore;
        this.fileRecordConsumer = fileRecordConsumer;
    }

    void readFile(MonitoredFile monitoredFile) {
        final Path filePath = monitoredFile.getPath();
        // object to pass metadata within
        FileRecord fileRecord = new FileRecord(monitoredFile.getPath());



        // !! trace log existing position
        FileChannel fileChannel = fileChannelCache.acquire(monitoredFile.getPath());
        if (fileChannel == null) {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("Gave up on <[{}]> due to null FileChannel.",filePath);
            }
            stateStore.deleteOffset(monitoredFile.getPath());
            return;
        }

        try {
            LOGGER.info("fileChannel position <{}>", fileChannel.position());
            LOGGER.info("fileChannel size <{}>", fileChannel.size());
        } catch (IOException ignored) {

        }


        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32*1024);
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(1024*1024); // todo configurable


        long lastRecordEnd = stateStore.getOffset(monitoredFile.getPath());
        try {
            if (fileChannel.size() < lastRecordEnd) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                            "Path <[{}]> truncated: size: <{}> is LT position <{}>.",
                           filePath,
                            fileChannel.size(),
                            lastRecordEnd
                    );
                }
                lastRecordEnd = 0;
                stateStore.setOffset(monitoredFile.getPath(), lastRecordEnd);
            }

            LOGGER.info("lastRecordEnd <{}> for <{}>", lastRecordEnd, filePath);

            fileChannel.position(lastRecordEnd);
            fileRecord.setStartOffset(lastRecordEnd); // set initial startingPosition

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
                        throw new RuntimeException("CATTIA " + bytesRead);
                    }

                    if (b != '\n' && outputBuffer.position() != outputBuffer.capacity() - 1) {
                        outputBuffer.put(b);
                    }
                    else {
                        outputBuffer.put((byte) '\n');
                        outputBuffer.flip();
                        LOGGER.trace("Produced fileRecord at <{}>", fileChannel.position());
                        if(outputBuffer.remaining() == outputBuffer.capacity()) {
                            throw new RuntimeException("WAT");
                        }
                        byte[] bytes = new byte[outputBuffer.remaining()];
                        outputBuffer.get(bytes);

                        fileRecord.setEndOffset(fileChannel.position());
                        fileRecord.setRecord(bytes);
                        //LOGGER.info("bytesRead <{}>", bytesRead);
                        fileRecordConsumer.accept(fileRecord);

                        // record complete
                        stateStore.setOffset(monitoredFile.getPath(), fileChannel.position());

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
        finally {
            fileChannelCache.release(monitoredFile.getPath());
        }
    }

    @Override
    public void accept(MonitoredFile monitoredFile) {
        if(LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Accept path <[{}]> with status <{}>",
                    monitoredFile.getPath(),
                    monitoredFile.getStatus()
            );
        }

        LOGGER.info("<{}> entry for <{}>", monitoredFile.getStatus(), monitoredFile.getPath());
        switch (monitoredFile.getStatus()) {
            case SYNC_NEW:
                readFile(monitoredFile);
                break;
            case SYNC_MODIFIED:
                readFile(monitoredFile);
                break;
            case SYNC_DELETED:
                readFile(monitoredFile);
                readFile(monitoredFile);
                readFile(monitoredFile);
                readFile(monitoredFile);
                readFile(monitoredFile);
                fileChannelCache.invalidate(monitoredFile.getPath());
                stateStore.deleteOffset(monitoredFile.getPath());
                break;
            case SYNC_RECREATED:
                readFile(monitoredFile);
                fileChannelCache.invalidate(monitoredFile.getPath());
                stateStore.deleteOffset(monitoredFile.getPath());
                readFile(monitoredFile);
                break;
            default:
                throw new IllegalStateException("monitoredFile.getStatus() provided invalid state <" + monitoredFile.getStatus() + ">");
        }
        LOGGER.info("<{}> exit for <{}>", monitoredFile.getStatus(), monitoredFile.getPath());
    }
}
