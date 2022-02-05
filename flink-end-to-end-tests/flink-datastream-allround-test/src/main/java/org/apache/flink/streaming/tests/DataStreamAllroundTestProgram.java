/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files. This Job can be executed in both streaming and batch execution modes.
 *
 * <p>The input is a [list of] plain text file[s] with lines separated by a newline character.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--input &lt;path&gt;</code>A list of input files and / or directories to read. If no
 *       input is provided, the program is run with default data from {@link}.
 *   <li><code>--discovery-interval &lt;duration&gt;</code>Turns the file reader into a continuous
 *       source that will monitor the provided input directories every interval and read any new
 *       files.
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 *   <li><code>--execution-mode &lt;mode&gt;</code>The execution mode (BATCH, STREAMING, or
 *       AUTOMATIC) of this pipeline.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Write a simple Flink DataStream program
 *   <li>Use tuple data types
 *   <li>Write and use a user-defined function
 * </ul>
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        class BufferBlockReader {
            private InputStream input;
            private long blockSize;
            private long currentPos;
            private int cursor;
            private int bufferSize = 0;
            private byte[] buffer = new byte[4096]; //4k buffer
            private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            private List<String> fields = new ArrayList<>();
            private HashSet<Integer> keptFields = null;
            private char delimiter;

            public BufferBlockReader(InputStream input, long blockSize, char delimiter, ArrayList<Object> kept) {
                this.input = input;
                this.blockSize = blockSize;
                this.delimiter = delimiter;
                ArrayList<Integer> keepIdx = new ArrayList<>();
                if (kept != null && kept.size() > 0) {
                    kept.stream().forEach(idx -> keepIdx.add((Integer) idx));
                    this.keptFields = new HashSet<>(keepIdx);
                }
            }

            public String[] readLine() throws IOException {
                outputStream.reset();
                fields.clear();
                int index = 0;
                while (true) {
                    if (cursor >= bufferSize) {
                        fillBuffer();
                        if (bufferSize == -1) {
                            if (outputStream.size() > 0) {
                                fields.add(outputStream.toString());
                            }
                            return fields.isEmpty() ? null : fields.toArray(new String[0]);
                        }
                    }
                    int start = cursor;
                    while (cursor < bufferSize) {
                        if (buffer[cursor] == delimiter) {
                            addField(start, index);
                            outputStream.reset();
                            start = cursor + 1;
                            index++;
                        } else if (buffer[cursor] == '\r' || buffer[cursor] == '\n') {
                            // If line ended with '\r\n', all the fields will be outputted when buffer[cursor] == '\r'
                            // And then the cursor will move to '\n' and output Tuple(null) in next readLine() call
                            // The behavior above is the same for either
                            // 1. the current buffer keeps '\r\n'
                            // 2. '\n' comes from the next fillBuffer() call
                            addField(start, index);
                            cursor++;
                            return fields.toArray(new String[0]);
                        }
                        cursor++;
                    }
                    outputStream.write(buffer, start, bufferSize - start);
                    currentPos += bufferSize - start;
                }
            }

            private void fillBuffer() throws IOException {
                bufferSize = input.read(buffer);
                cursor = 0;
            }


            private void addField(int start, int fieldIndex) {
                if (keptFields == null || keptFields.contains(fieldIndex)) {
                    if (cursor - start > 0) {
                        outputStream.write(buffer, start, cursor - start);
                        fields.add(outputStream.toString());
                    } else if (outputStream.size() > 0) {
                        fields.add(outputStream.toString());
                    } else {
                        fields.add(null);
                    }
                }
                currentPos += cursor - start + 1;
            }

            public boolean hasNext() throws IOException {
                return currentPos <= blockSize && bufferSize != -1;
            }

            public void close() throws IOException {
                input.close();
            }
        }

        // final CLI params = CLI.fromArgs(args);

        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Apache Flink’s unified approach to stream and batch processing means that a DataStream
        // application executed over bounded input will produce the same final results regardless
        // of the configured execution mode. It is important to note what final means here: a job
        // executing in STREAMING mode might produce incremental updates (think upserts in
        // a database) while in BATCH mode, it would only produce one final result at the end. The
        // final result will be the same if interpreted correctly, but getting there can be
        // different.
        //
        // The “classic” execution behavior of the DataStream API is called STREAMING execution
        // mode. Applications should use streaming execution for unbounded jobs that require
        // continuous incremental processing and are expected to stay online indefinitely.
        //
        // By enabling BATCH execution, we allow Flink to apply additional optimizations that we
        // can only do when we know that our input is bounded. For example, different
        // join/aggregation strategies can be used, in addition to a different shuffle
        // implementation that allows more efficient task scheduling and failure recovery behavior.
        //
        // By setting the runtime mode to AUTOMATIC, Flink will choose BATCH if all sources
        // are bounded and otherwise STREAMING.
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // This optional step makes the input parameters
        // available in the Flink UI.
        // env.getConfig().setGlobalJobParameters(params);

        int sourceParallelism = 1;
        int filterParallelism = 1;
        int aggregateParallelism = 1;
        int sinkParallelism = 1;

        abstract class MySource implements ParallelSourceFunction<String[]>, CheckpointedFunction{}

        String path = "hdfs://10.128.0.10:8020/tpch-30G/orders.tbl";
        DataStream<String[]> dynamicSource = env.readTextFile(path).process(new ProcessFunction<String, String[]>() {
            @Override
            public void processElement(
                    String value,
                    ProcessFunction<String, String[]>.Context ctx,
                    Collector<String[]> out) throws Exception {
                out.collect(value.split("\\|"));
            }
        }).setParallelism(sourceParallelism);


//        DataStream<String[]> dynamicSource = env.addSource(new RichParallelSourceFunction<String[]>(){
//            FSDataInputStream stream = null;
//            long blockSize = 0;
//            long startOffset = 0;
//
//            @Override
//            public void setRuntimeContext(RuntimeContext t) {
//                super.setRuntimeContext(t);
//                try {
//                    System.out.println("start init");
//                    Configuration conf = new Configuration();
//                    FileSystem fs2 = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf);
//                    Long totalBytes = fs2.getFileStatus(new Path("/tpch-30G/orders.tbl")).getLen();
//                    stream = fs2.open(new Path("/tpch-30G/orders.tbl"));
//                    startOffset = totalBytes / sourceParallelism * t.getIndexOfThisSubtask();
//                    stream.seek(startOffset);
//                    if (t.getIndexOfThisSubtask() != sourceParallelism - 1){
//                        blockSize = (totalBytes / sourceParallelism * (t.getIndexOfThisSubtask() + 1))-startOffset;
//                    } else{
//                        blockSize = totalBytes-startOffset;
//                    }
//                }catch(Exception e){
//                    e.printStackTrace();
//                }
//            }
//
//            @Override
//            public void run(SourceContext<String[]> ctx) throws Exception {
//                System.out.println("start reading");
//                String l = stream.readLine();
//                System.out.println("end reading content = "+l);
//                BufferBlockReader reader = new BufferBlockReader(stream, blockSize,'|',null);
//                if(startOffset !=0) reader.readLine();
//                String line = stream.readLine();
//                System.out.println(line);
//                while(reader.hasNext())   {
//                    String[] arr = reader.readLine();
//                    ctx.collect(arr);
//                }
//                reader.close();
////                for(int i = 0; i<1000; ++i){
////                    ctx.collect(new String[]{String.valueOf(i),String.valueOf(i)});
////                }
//                ctx.emitWatermark(new Watermark(999));
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(sourceParallelism);

        SingleOutputStreamOperator<String[]> filter = dynamicSource.filter(new FilterFunction<String[]>() {
            @Override
            public boolean filter(String[] value) throws Exception {
                return Objects.equals(value[1], "F");
            }
        }).setParallelism(filterParallelism);
        filter.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                return key.hashCode();
            }
        },new KeySelector<String[], String>() {

            @Override
            public String getKey(String[] value) throws Exception {
                return value[1];
            }
        }).process(new ProcessFunction<String[], Integer>() {

            int count = 0;

            @Override
            public void processElement(
                    String[] value,
                    ProcessFunction<String[], Integer>.Context ctx,
                    Collector<Integer> out) throws Exception {
                count++;
            }

            @Override
            public void onTimer(
                    long timestamp,
                    ProcessFunction<String[], Integer>.OnTimerContext ctx,
                    Collector<Integer> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                if(timestamp == 999) out.collect(count);
            }

        }).setParallelism(aggregateParallelism).process(new ProcessFunction<Integer, Integer>() {
            int sum = 0;
            int seen = 0;
            @Override
            public void processElement(
                    Integer value,
                    ProcessFunction<Integer, Integer>.Context ctx,
                    Collector<Integer> out) throws Exception {
                seen++;
                sum += value;
                if(seen == aggregateParallelism){
                    out.collect(sum);
                }
            }
        }).setParallelism(1).print();

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("workflow");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}

