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
import com.teragrep.rlo_13.statestore.LMDBStateStore;
import com.teragrep.rlo_13.statestore.StateStore;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class StatefulFileReader implements Supplier<Consumer<MonitoredFile>>, AutoCloseable {

    private final FileChannelCache fileChannelCache;
    private final StateStore stateStore;

    private final Supplier<Consumer<FileRecord>> fileRecordConsumerSupplier;

    public StatefulFileReader(Path stateStorePath, Supplier<Consumer<FileRecord>> fileRecordConsumerSupplier) {
        this.stateStore = new LMDBStateStore(stateStorePath);
        this.fileChannelCache = new FileChannelCache();
        this.fileRecordConsumerSupplier = fileRecordConsumerSupplier;
    }


    @Override
    public Consumer<MonitoredFile> get() {
        return new MonitoredFileConsumer(fileChannelCache, stateStore, fileRecordConsumerSupplier.get());
    }

    @Override
    public void close() {
        fileChannelCache.close();
        stateStore.close();
    }
}
