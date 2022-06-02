/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Example illustrating iterations in Flink streaming.
 *
 * <p>The program sums up random numbers and counts additions it performs to reach a specific
 * threshold in an iterative streaming fashion.
 *
 * <p>This example shows how to use:
 *
 * <ul>
 *   <li>streaming iterations,
 *   <li>buffer timeout to enhance latency,
 *   <li>directed outputs.
 * </ul>
 */
public class IterateExample {

//    private static List<String> tags = Arrays.asList("1","1","1","1","1","1","2","3","4","5","6","7","8","9","10","11","12");
//    private static List<String> tags = Arrays.asList("1","1","2","3","4","5","6","7","8","9","10","11","12","13","13","14","14");
private static List<String> tags = Arrays.asList("1","2","3","4","5","6","7","8","9","10","11","12");
    private static Integer currentTagIdx = 0;

    private static String getNextTag() {
        if(currentTagIdx>=tags.size()) {currentTagIdx = 0;}
        String ret = tags.get(currentTagIdx);
        currentTagIdx++;
        return ret;
    }

    public static class KeyPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {

        //final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        int sourceParallelism = 2;
        int filterParallelism = 12;
        int aggregateParallelism = 1;

        abstract class MyAggregate  extends AbstractStreamOperator<Long> implements OneInputStreamOperator<String[], Long> {}

        String path = "hdfs://10.128.0.25:8020/tpch-30G/orders.tbl";
        DataStream<String[]> dynamicSource = env.readTextFile(path).setParallelism(6).process(new ProcessFunction<String, String[]>() {
            boolean first = false;
            @Override
            public void processElement(
                    String value,
                    ProcessFunction<String, String[]>.Context ctx,
                    Collector<String[]> out) throws Exception {
                if(!first){
                    System.out.println(Arrays.toString(value.split("\\|")));
                    first=true;
                }
                try{
                    String[] cols = value.split("\\|");
                    cols[0] = getNextTag();
                    out.collect(cols);
                }catch (Exception e){
                    System.out.println("Error: "+Arrays.toString(value.split("\\|")));
                }
            }
        }).setParallelism(6).partitionCustom(new KeyPartitioner(), value -> Integer.parseInt(value[0]));

        DataStream<String[]> filter1 = dynamicSource.filter(new FilterFunction<String[]>() {
            @Override
            public boolean filter(String[] value) throws Exception {
                List<String> keywords =  Arrays.asList("asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS");
                int countNum = 0;
                for(int k=0;k < 30;++k) {
                    for (int i = 0; i < keywords.size(); i++) {
                        if (keywords.get(i).contains(value[1]+k)) {
                            countNum++;
                        }
                    }
                }
                if(countNum > 23) {
                    System.out.println("More than 23");
                }
                return true;
            }
        }).setParallelism(6).partitionCustom(new KeyPartitioner(), value -> Integer.parseInt(value[0])+2);

        filter1.filter(new FilterFunction<String[]>() {
            @Override
            public boolean filter(String[] value) throws Exception {
                List<String> keywords =  Arrays.asList("asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS","asda","SFSD","DFSD","FDFDS");
                int countNum = 0;
                for(int k=0;k < 32;++k) {
                    for (int i = 0; i < keywords.size(); i++) {
                        if (keywords.get(i).contains(value[1]+k)) {
                            countNum++;
                        }
                    }
                }
                if(countNum > 23) {
                    System.out.println("More than 23");
                }
                return Objects.equals(value[2], "F23");
            }
        }).setParallelism(6).print();

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("workflow");
    }
}
