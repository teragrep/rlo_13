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

import java.nio.file.Path;
import java.util.Arrays;

public class FileRecord {

    private final String path;
    private final String directory;
    private final String filename;
    private long startOffset; // exclusive
    private long endOffset; // inclusive
    private byte[] record;

    FileRecord(Path path) {
        this.path = path.toString();
        this.directory = path.getParent().toString();
        this.filename = path.getFileName().toString();
    }

    void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    void setRecord(byte[] record) {
        this.record = record;
    }

    public String getPath() {
        return path;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public byte[] getRecord() {
        return record;
    }

    public String getDirectory() {
        return directory;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public String toString() {
        return "FileRecord{" +
                "path='" + path + '\'' +
                ", directory='" + directory + '\'' +
                ", filename='" + filename + '\'' +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", record=" + Arrays.toString(record) +
                '}';
    }
}
