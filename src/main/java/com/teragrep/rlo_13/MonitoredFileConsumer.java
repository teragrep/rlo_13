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

    void readFile(Path filePath) {
        // object to pass metadata within
        FileRecord fileRecord = new FileRecord(filePath);

        long absolutePosition = stateStore.getOffset(filePath);
        long lastRecordStart = absolutePosition;
        // !! trace log existing position
        FileChannel fileChannel = fileChannelCache.acquire(filePath);
        if (fileChannel == null) {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("Gave up on <[{}]> due to null FileChannel.", filePath);
            }
            stateStore.deleteOffset(filePath);
            return;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32*1024);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            long fileChannelSize = fileChannel.size();
            if (fileChannelSize < absolutePosition) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                            "Path <[{}]> truncated: size: <{}> is LT position <{}>.",
                            filePath,
                            fileChannelSize,
                            absolutePosition
                    );
                }
                absolutePosition = 0;
                stateStore.setOffset(filePath, absolutePosition);
            }
            fileChannel.position(absolutePosition);
            fileRecord.setStartOffset(absolutePosition); // set initial startingPosition
            // note that this does not read all the changes, it will be re-read when something happens during read
            while (fileChannel.position() < fileChannelSize) {
                fileChannel.read(byteBuffer);
                byteBuffer.flip(); // reading
                while (byteBuffer.hasRemaining()) {
                    byte b = byteBuffer.get();
                    absolutePosition++;
                    if (b != 10) { // newline
                        byteArrayOutputStream.write(b);
                    }
                    else {
                        byteArrayOutputStream.write(10);
                        LOGGER.trace("Produced fileRecord at <{}>", absolutePosition);
                        fileRecord.setEndOffset(absolutePosition);
                        fileRecord.setRecord(byteArrayOutputStream.toByteArray());
                        fileRecordConsumer.accept(fileRecord);
                        byteArrayOutputStream.reset();
                        fileRecord.setStartOffset(absolutePosition); // next if any
                        lastRecordStart = absolutePosition;
                    }
                }
                byteBuffer.clear();
            }
            // persistence at lastRecordStart, partial ones will be re-read
            stateStore.setOffset(filePath, lastRecordStart);
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
        finally {
            fileChannelCache.release(filePath);
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

        switch (monitoredFile.getStatus()) {
            case SYNC_NEW:
                readFile(monitoredFile.getPath());
                break;
            case SYNC_MODIFIED:
                readFile(monitoredFile.getPath());
                break;
            case SYNC_DELETED:
                readFile(monitoredFile.getPath());
                fileChannelCache.invalidate(monitoredFile.getPath());
                stateStore.deleteOffset(monitoredFile.getPath());
                break;
            case SYNC_RECREATED:
                readFile(monitoredFile.getPath());
                fileChannelCache.invalidate(monitoredFile.getPath());
                stateStore.deleteOffset(monitoredFile.getPath());
                readFile(monitoredFile.getPath());
                break;
            default:
                throw new IllegalStateException("monitoredFile.getStatus() provided invalid state <" + monitoredFile.getStatus() + ">");
        }
    }
}
