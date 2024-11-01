/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

class Measurement {
    public int min = Integer.MAX_VALUE;
    public int max = Integer.MIN_VALUE;
    public int sum = 0;
    public int count = 0;

    public Measurement(int value) {
        this.min = value;
        this.max = value;
        this.sum = value;
        this.count = 1;
    }

    @Override
    public String toString() {
        return round(min / 10.0) + "/" + round(max / 10.0) + "/" + round((sum / 10.0) / count);
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

}

class FastKey implements Comparable<FastKey> {
    private byte[] bytes;
    private int hash;

    private static final Charset charset = Charset.forName("UTF-8");

    public FastKey(MemorySegment segment) {
        bytes = segment.toArray(ValueLayout.OfByte.JAVA_BYTE);

        long hash = 0x811C9DC5;
        long prime = 0x01000193;

        for (byte b : bytes) {
            hash = hash ^ b;
            hash *= prime;
        }

        this.hash = (int) hash;
    }

    @Override
    public int hashCode() {
        return this.hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FastKey)) {
            return false;
        }

        return Arrays.equals(bytes, ((FastKey) obj).bytes);
    }

    @Override
    public int compareTo(FastKey o) {
        return Arrays.compare(bytes, o.bytes);
    }

    @Override
    public String toString() {
        return new String(bytes, charset);
    }
}

class ComputeMeasurementsPartTask implements Callable<Map<FastKey, Measurement>> {

    private int start;
    private int end;
    private static final Charset charset = Charset.forName("UTF-8");
    private MemorySegment segment;

    private byte getByte(int index) {
        return segment.get(ValueLayout.OfByte.JAVA_BYTE, index);
    }

    private long getLong(int index) {
        return segment.get(ValueLayout.OfLong.JAVA_LONG, index);
    }

    public ComputeMeasurementsPartTask(int start, int end, int limit, FileChannel channel) throws IOException {

        this.segment = channel.map(FileChannel.MapMode.READ_ONLY, start, limit - start, Arena.global());
        int s = start, s2 = start, e = end;

        if (s != 0) {
            while (getByte(s - s2) != 0x0a) {
                s++;
            }
            s++;
        }

        while (e < limit && getByte(e - s2) != 0x0a) {
            e++;
        }

        int prefix = s % 8;
        MemorySegment padded = Arena.global().allocate(e - s + prefix);
        padded.asSlice(prefix).copyFrom(segment.asSlice(s - s2, e - s));

        this.segment = padded;
        this.start = prefix;
        this.end = e - s + prefix;
    }

    private void doActualWork(int start, int end, Map<FastKey, Measurement> measurements) {

        int splitIndex = start;
        while (getByte(splitIndex) != 0x3b) {
            splitIndex++;
        }

        var key = new FastKey(segment.asSlice(start, splitIndex - start));

        boolean negative = false;
        int ind = splitIndex + 1;

        if (getByte(ind) == (byte) '-') {
            negative = true;
            ind++;
        }

        int v = 0;

        if (end - ind == 4) {
            v = v * 10 + getByte(ind++) - '0';
            v = v * 10 + getByte(ind++) - '0';
            ind++; // '.'
            v = v * 10 + getByte(ind) - '0';
        }
        else {
            v = getByte(ind++) - '0';
            ind++; // '.'
            v = v * 10 + getByte(ind) - '0';
        }

        int val = negative ? -v : v;

        var _v = measurements.get(key);

        if (_v != null) {
            if (val < _v.min) {
                _v.min = val;
            }

            if (val > _v.max) {
                _v.max = val;
            }

            _v.sum += val;
            _v.count++;
        }
        else {
            measurements.put(key, new Measurement(val));
        }
    }

    @Override
    public Map<FastKey, Measurement> call() throws Exception {
        var measurements = new HashMap<FastKey, Measurement>();

        int prev = start;
        for (int i = 0; i < end; i += 8) {
            if (i + 8 < end) {
                long word = getLong(i);

                long match = word ^ 0x0a0a0a0a0a0a0a0aL;
                long line = (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);

                if (line == 0) {
                    i += 8;
                    continue;
                }
                int next = i + (Long.numberOfTrailingZeros(line) >>> 3);
                doActualWork(prev, next, measurements);
                prev = next + 1;
            }
            else {
                doActualWork(prev, end, measurements);
            }

        }

        return measurements;
    }
}

public class CalculateAverage_dg {

    public static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {

        FileChannel channel = FileChannel.open(Path.of(CalculateAverage_dg.FILE), StandardOpenOption.READ);

        var parts = new ArrayList<Map<FastKey, Measurement>>();
        var futures = new ArrayList<Future<Map<FastKey, Measurement>>>();

        int fileSize = (int) Files.size(Path.of(FILE));
        int chunkSize = 200 * 1024 * 1024;
        int noOfThreads = fileSize / chunkSize;

        for (int i = 0; i < noOfThreads; i++) {
            int start = i * chunkSize;
            int end = Math.min((i + 1) * chunkSize, fileSize);
            int limit = Math.min((i + 1) * chunkSize + 1024, fileSize);

            var task = new ComputeMeasurementsPartTask(start, end, limit, channel);
            futures.add(ForkJoinPool.commonPool().submit(task));
        }

        for (var future : futures) {
            parts.add(future.get());
        }

        var measurements = parts.stream().flatMap(map -> map.entrySet().stream())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> {
                                    e1.min = Integer.min(e1.min, e2.min);
                                    e1.max = Integer.max(e1.max, e2.max);

                                    e1.sum += e2.sum;
                                    e1.count += e2.count;

                                    return e1;
                                }));

        var sorted = new TreeMap<>(measurements);

        System.out.println(sorted);

    }
}
