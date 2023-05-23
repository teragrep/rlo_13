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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.READ;

class FileChannelCache implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileChannelCache.class);
    private final Cache<Path, FileChannel> cache;
    private final Map<Path, FileChannel> activeFileChannels = new HashMap<>();

    private final Lock lock = new ReentrantLock();

    FileChannelCache() {
        RemovalListener<Path, FileChannel> listener;
        listener = removalNotification -> {
        String cause = removalNotification.getCause().name();
        LOGGER.trace("Entry removed because: {}", cause);


            try {
                if (!activeFileChannels.containsKey(removalNotification.getKey())) {
                    // inactive, throw it out
                    LOGGER.trace("Removal triggered for path <[{}]>", removalNotification.getKey());
                    removalNotification.getValue().close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        this.cache = CacheBuilder
                .newBuilder()
                .maximumSize(512)
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .removalListener(listener)
                .build();
    }

    FileChannel acquire(Path path) {
        lock.lock();
        try {
            LOGGER.trace("Acquiring path <[{}]>", path);

            FileChannel fileChannel = cache.getIfPresent(path);
            if (fileChannel == null) {
                LOGGER.trace("Acquire opens a new FileChannel. Not cached!");
                fileChannel = FileChannel.open(path, READ);
                cache.put(path, fileChannel);
            }

            activeFileChannels.put(path, fileChannel);
            return fileChannel;
        }
        catch (AccessDeniedException accessDeniedException) {
            LOGGER.warn("Reading of inaccessible file <[{}]> skipped.", path);
            return null;
        }
        catch (NoSuchFileException noSuchFileException) {
            LOGGER.warn("Reading of non-present file <[{}]> skipped.", path);
            return null;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            LOGGER.trace("Acquired path <[{}]>", path);
            lock.unlock();
        }
    }

    void release(Path path) {
        lock.lock();
        try {
            LOGGER.trace("Releasing path <[{}]>", path);
            FileChannel fileChannel = activeFileChannels.get(path);
            if (fileChannel == null) {
                throw new IllegalStateException("Attempt to remove inactive path <["+path+"]>");
            }

            if (cache.getIfPresent(path) == null) {
                LOGGER.trace("Path <[{}]> not present in FileChannelCache, closing!", path);
                // no longer in cache, begone now!
                try {
                    fileChannel.close();
                }
                catch (IOException ioException) {
                    LOGGER.warn("Close on FileChannel for path <[{}]> caused:", path, ioException);
                }
            }
            activeFileChannels.remove(path);
        }
        finally {
            LOGGER.trace("Released path <[{}]>", path);
            lock.unlock();
        }
    }

    void invalidate(Path path) {
        lock.lock();
        try {
            if (activeFileChannels.containsKey(path)) {
                throw new IllegalStateException("Must not invalidate active path");
            }
            cache.invalidate(path);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            cache.invalidateAll();
            cache.cleanUp();
        }
        finally {
            lock.unlock();
        }
    }
}
