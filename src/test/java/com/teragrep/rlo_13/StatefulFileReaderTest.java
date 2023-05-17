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

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class StatefulFileReaderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulFileReaderTest.class);

    @Test
    @EnabledIfSystemProperty(named = "runManualTmpTest", matches = "true")
    public void manualTmpTest() {

        Supplier<Consumer<FileRecord>> consumerSupplier = () -> fileRecord -> LOGGER.info(new String(fileRecord.getRecord(), StandardCharsets.UTF_8));
        //Supplier<Consumer<FileRecord>> consumerSupplier = () -> fileRecord -> LOGGER.info(fileRecord.toString());

        try (StatefulFileReader statefulFileReader =
                     new StatefulFileReader(
                             Paths.get("/tmp/rlo_13_state"),
                             consumerSupplier
                     )
        ) {
            DirectoryEventWatcher dew = new DirectoryEventWatcher(
                    Paths.get("/tmp/rlo_13_test"),
                    false,
                    Pattern.compile("^.*$"),
                    statefulFileReader
            );

            dew.watch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
