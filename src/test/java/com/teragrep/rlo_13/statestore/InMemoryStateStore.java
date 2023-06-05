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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class InMemoryStateStore implements StateStore {

    private final Map<Path, Long> pathOffsetMap = new HashMap<>();

    @Override
    public void setOffset(Path path, long offset) {
        pathOffsetMap.put(path, offset);
    }

    @Override
    public void deleteOffset(Path path) {
        pathOffsetMap.remove(path);
    }

    @Override
    public long getOffset(Path path) {
        Long offset = pathOffsetMap.get(path);
        if (offset == null) {
            return 0;
        }
        else {
            return offset;
        }

    }

    @Override
    public void close() {
        // no-op
    }
}
