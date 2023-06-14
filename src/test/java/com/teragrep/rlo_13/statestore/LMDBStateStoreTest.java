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
package com.teragrep.rlo_13.statestore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LMDBStateStoreTest {

    @Test
    public void testGet() throws IOException {
        Path storePath = Paths.get("target/LMDBStateStoreTest#teStore");

        Files.createDirectories(storePath);

        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            Path testPath = Paths.get("/some/testGet");
            lss.setOffset(testPath, 123456);

            Assertions.assertEquals(123456,lss.getOffset(testPath));
        }
    }

    @Test
    public void testNoPath() throws IOException {
        Path storePath = Paths.get("target/LMDBStateStoreTest#teStore");

        Files.createDirectories(storePath);

        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            Path notSetPath = Paths.get("/some/testNoPath");

            Assertions.assertEquals(0,lss.getOffset(notSetPath));
        }
    }

    @Test
    public void testSetGetDeletePath() throws IOException {
        Path storePath = Paths.get("target/LMDBStateStoreTest#teStore");

        Files.createDirectories(storePath);

        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            Path testPath = Paths.get("/some/testSetGetDeletePath");

            // not yet set
            Assertions.assertEquals(0,lss.getOffset(testPath));

            // set it
            lss.setOffset(testPath, 123456);
            Assertions.assertEquals(123456,lss.getOffset(testPath));

            // delete it
            lss.deleteOffset(testPath);

            // no longer set
            Assertions.assertEquals(0,lss.getOffset(testPath));
        }
    }

    @Test
    public void testSetGetCloseGetPath() throws IOException {
        Path storePath = Paths.get("target/LMDBStateStoreTest#MoreStore");

        Path testPath = Paths.get("/some/testSetGetCloseGetPath");

        Files.createDirectories(storePath);

        // open store
        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            // not yet set
            Assertions.assertEquals(0,lss.getOffset(testPath));

            // set it
            lss.setOffset(testPath, 123456);
            Assertions.assertEquals(123456,lss.getOffset(testPath));
        } // closes

        // open second time
        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            // get already set
            Assertions.assertEquals(123456,lss.getOffset(testPath));

            // delete
            lss.deleteOffset(testPath);
        } // closes


        // open third time
        try (LMDBStateStore lss = new LMDBStateStore(storePath)) {
            // get deleted
            Assertions.assertEquals(0,lss.getOffset(testPath));
        } // closes
    }
}
