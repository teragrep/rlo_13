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

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

public class StateStore implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateStore.class);

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;

    private final KeyHashProvider keyHashProvider = new KeyHashProvider();
    public StateStore(Path stateStorePath) {
        env =
                create()
                        // 2 * 1024 * 1024 * 1024 / (64 (sha-256) + 8 (Long.BYTES) = 29M files
                        .setMapSize((long) 2 * 1024 * 1024 * 1024) //
                        .setMaxDbs(2)
                        .open(stateStorePath.toFile());

        db = env.openDbi("StateStore", MDB_CREATE);
        getVersion();
    }

    private long getVersion() {
        Dbi<ByteBuffer> versionDb = env.openDbi("VersionStore", MDB_CREATE);

        byte[] versionStringBytes = "StateStoreVersion".getBytes(UTF_8);
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put(versionStringBytes).flip();

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = versionDb.get(txn, key);
            if (found == null) {
                // new DB
                long version = 1;
                LOGGER.trace("Created new StateStore with version <{}>", version);
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Long.BYTES);
                byteBuffer.putLong(version).flip();
                versionDb.put(key,byteBuffer);
                return version;
            }
            else {
                final ByteBuffer fetchedVal = txn.val();
                long version = fetchedVal.getLong();
                LOGGER.trace("Found existing StateStore with version <{}>", version);
                return version;
            }
        }
    }

    public void setOffset(Path path, long offset) {

        final ByteBuffer val = allocateDirect(Long.BYTES);
        val.putLong(offset).flip();

        ByteBuffer key = keyHashProvider.getKey(path);
        db.put(key, val);
    }

    public void deleteOffset(Path path) {
        ByteBuffer key = keyHashProvider.getKey(path);
        db.delete(key);
    }

    public long getOffset(Path path) {
        ByteBuffer key = keyHashProvider.getKey(path);

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            if (found == null) {
                return 0;
            }
            else {
                final ByteBuffer fetchedVal = txn.val();
                return fetchedVal.getLong();
            }
        }

    }



    @Override
    public void close() {
        env.close();
    }
}
