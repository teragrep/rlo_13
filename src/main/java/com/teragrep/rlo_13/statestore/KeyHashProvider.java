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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

class KeyHashProvider {

    private final LoadingCache<Path, ByteBuffer> cache;
    KeyHashProvider() {
        CacheLoader<Path, ByteBuffer> loader = new CacheLoader<Path, ByteBuffer>() {
            @Override
            public ByteBuffer load(Path path) {
                byte[] pathHash = hash(path.toString().getBytes(UTF_8));
                final ByteBuffer key = allocateDirect(pathHash.length); // env.getMaxKeySize()
                key.put(pathHash).flip();
                return key;
            }
        };

        this.cache = CacheBuilder
                .newBuilder()
                .maximumSize(512)
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build(loader);
    }

    private byte[] hash(byte[] path) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return sha256.digest(path);
        }
        catch (NoSuchAlgorithmException noSuchAlgorithmException) {
            throw new RuntimeException(noSuchAlgorithmException);
        }
    }

    ByteBuffer getKey(Path path) {
        try {
            ByteBuffer bytebuffer = cache.get(path);
            bytebuffer.position(0);
            return bytebuffer;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
