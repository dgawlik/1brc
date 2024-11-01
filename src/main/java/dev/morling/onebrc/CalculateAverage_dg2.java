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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class CalculateAverage_dg2 {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        public double min = Double.MAX_VALUE;
        public double max = Double.MIN_VALUE;
        public double sum = 0;
        public double count = 0;

        public Measurement(double value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(max) + "/" + round(sum / count);
        }
    }

    public static void main(String[] args) throws Exception {

        ConcurrentHashMap<String, Measurement> measurements = new ConcurrentHashMap<>();

        Files.lines(Path.of(FILE)).parallel().forEach(line -> {

            int split = line.indexOf(";");
            String key = line.substring(0, split);
            double val = Double.parseDouble(line.substring(split + 1));

            measurements.compute(key, (k, _v) -> {
                if (_v == null) {
                    return new Measurement(val);
                }

                _v.min = Double.min(_v.min, val);
                _v.max = Double.max(_v.max, val);

                _v.sum += val;
                _v.count++;

                return _v;
            });
        });

        var sorted = new TreeMap<>(measurements);

        System.out.println(sorted);

    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

}
