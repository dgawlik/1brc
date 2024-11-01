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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

// class FastSpliterator implements Spliterator<Integer> {
//
// public static ConcurrentLinkedQueue<Map<String, Measurement>> ALL_MEASUREMENTS = new ConcurrentLinkedQueue<>();
// public static Map<String, Measurement> measurements = new HashMap<>();
//
//
// private ByteBuffer buffer;
// private final byte[] bytes;
// private int start;
// private final int end;
// private int current;
//
// public FastSpliterator(byte[] bytes, int start, int end, HashMap<String, Measurement> measurements) {
// this.buffer = ByteBuffer.wrap(bytes, start, end - start);
// this.bytes = bytes;
// this.start = start;
// this.end = end;
// this.current = start;
// this.measurements = measurements;
// }
//
// @Override
// public boolean tryAdvance(Consumer<? super Integer> action) {
// if (current >= end) {
// ALL_MEASUREMENTS.add(measurements);
// return false;
// }
//
// int next;
//
// int prev = current;
// int i = current;
// while (true) {
// if (i + 8 > end) {
// next = end;
// }
// else {
// long word = buffer.getLong();
//
// long match = word ^ 0x0a0a0a0a0a0a0a0aL;
// long line = (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
//
// if (line == 0) {
// i += 8;
// continue;
// }
//
// next = i + (Long.numberOfLeadingZeros(line) >>> 3) + 1;
// buffer.position(next);
// }
//
// int splitIndex = prev;
// while (bytes[splitIndex] != ';') {
// splitIndex++;
// }
//
// var key = new String(Arrays.copyOfRange(bytes, start, splitIndex), Charset.forName("UTF-8"));
//
// boolean negative = false;
// int ind = splitIndex + 1;
// int v = 0;
//
// if (bytes[ind] == (byte) '-') {
// negative = true;
// ind++;
// }
//
// v = bytes[ind++] - '0';
//
// if (bytes[ind] == (byte) '.') {
// ind++;
//
// v = v * 10 + bytes[ind] - '0';
// }
// else {
// v = v * 10 + bytes[ind++] - '0';
// ind++; // '.'
// v = v * 10 + bytes[ind] - '0';
// }
//
// int val = negative ? -v : v;
//
// measurements.compute(key, (k, _v) -> {
// if (_v == null) {
// return new Measurement(val);
// }
//
// _v.min = Integer.min(_v.min, val);
// _v.max = Integer.max(_v.max, val);
//
// _v.sum += val;
// _v.count++;
//
// return _v;
// });
//
// current = next;
//
// return true;
// }
// }
//
// @Override
// public Spliterator<Integer> trySplit() {
// int lo = current;
// int hi = end;
// int mid = (hi + lo) >>> 1;
//
// if (hi - lo < 100000) {
// return null;
// }
//
// while (mid < hi) {
// if (bytes[mid] == 0x0a) {
// break;
// }
// mid++;
// }
//
// if (mid + 1 >= hi) {
// return null;
// }
//
// current = mid + 1;
// buffer = ByteBuffer.wrap(bytes, current, end - current);
//
// return new FastSpliterator(bytes, lo, mid, new HashMap<>());
// }
//
// @Override
// public long estimateSize() {
// return end - current;
// }
//
// @Override
// public int characteristics() {
// return NONNULL | IMMUTABLE;
// }
// }

class FastKey implements Comparable<FastKey> {
    public byte[] bytes;

    private static final Charset charset = Charset.forName("UTF-8");

    public FastKey(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        long hash = 0x811C9DC5;
        long prime = 0x01000193;

        for (byte b : bytes) {
            hash = hash ^ b;
            hash *= prime;
        }

        return (int) hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FastKey)) {
            return false;
        }

        FastKey other = (FastKey) obj;

        return Arrays.equals(bytes, other.bytes);
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
    private byte[] bytes;
    private ByteBuffer buffer;
    private static final Charset charset = Charset.forName("UTF-8");

    public ComputeMeasurementsPartTask(byte[] bytes, int start, int end) {
        this.bytes = bytes;
        this.start = start;
        this.end = end;

        if (start != 0) {
            while (bytes[this.start++] != 0x0a)
                ;
        }

        while (this.end != bytes.length && bytes[this.end] != 0x0a) {
            this.end++;
        }

        buffer = ByteBuffer.wrap(bytes, this.start, this.end - this.start);
    }

    @Override
    public Map<FastKey, Measurement> call() throws Exception {
        var measurements = new HashMap<FastKey, Measurement>();

        int i = start;
        int prev = start;
        int next;
        while (true) {
            if (i + 8 > end) {
                next = end;
            }
            else {
                long word = buffer.getLong();

                long match = word ^ 0x0a0a0a0a0a0a0a0aL;
                long line = (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);

                if (line == 0) {
                    i += 8;
                    continue;
                }

                next = i + (Long.numberOfLeadingZeros(line) >>> 3) + 1;

                int splitIndex = prev;
                while (bytes[splitIndex] != 0x3b) {
                    splitIndex++;
                }

                var key = new FastKey(Arrays.copyOfRange(bytes, prev, splitIndex));

                boolean negative = false;
                int ind = splitIndex + 1;
                int v = 0;

                if (bytes[ind] == (byte) '-') {
                    negative = true;
                    ind++;
                }

                v = bytes[ind++] - '0';

                if (bytes[ind] == (byte) '.') {
                    ind++;

                    v = v * 10 + bytes[ind] - '0';
                }
                else {
                    v = v * 10 + bytes[ind++] - '0';
                    ind++; // '.'
                    v = v * 10 + bytes[ind] - '0';
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

                i = prev = next;
                buffer.position(next);
            }

            if (next >= end) {
                break;
            }

        }

        return measurements;
    }
}

public class CalculateAverage_dg {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {
        var bytes = Files.readAllBytes(Path.of(FILE));

        var parts = new ArrayList<Map<FastKey, Measurement>>();
        var futures = new ArrayList<Future<Map<FastKey, Measurement>>>();

        var noOfThreads = Runtime.getRuntime().availableProcessors() / 2;

        int chunkSize = bytes.length / noOfThreads;

        for (int i = 0; i < noOfThreads; i++) {
            int start = i * chunkSize;
            int end = Math.min((i + 1) * chunkSize, bytes.length);

            var task = new ComputeMeasurementsPartTask(bytes, start, end);
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
