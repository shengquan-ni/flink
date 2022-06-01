/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Simple example for demonstrating the use of SQL on a table backed by a {@link DataStream} in Java
 * DataStream API.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>convert two bounded data streams to tables,
 *   <li>register a table as a view under a name,
 *   <li>run a stream SQL query on registered and unregistered tables,
 *   <li>and convert the table back to a data stream.
 * </ul>
 *
 * <p>The example executes a single Flink job. The results are written to stdout.
 */
public final class StreamSQLExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // with Table API in batch mode and global parallelism 12. Sort -> 1 worker, but aggregate -> 12 workers
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        String path = "hdfs://10.128.0.10:8020/dsb-30G/customer.dat";
        DataStream<Tuple2<String,String>> dynamicSource = env.readTextFile(path).setParallelism(12).process(new ProcessFunction<String, Tuple2<String,String>>() {
            boolean first = false;
            @Override
            public void processElement(
                    String value,
                    ProcessFunction<String, Tuple2<String,String>>.Context ctx,
                    Collector<Tuple2<String,String>> out) throws Exception {
                if(!first){
                    System.out.println(Arrays.toString(value.split("\\|")));
                    first=true;
                }
                try{
                    out.collect(new Tuple2<>(value.split("\\|")[0],value.split("\\|")[12]));
                }catch (Exception e){
                    System.out.println("Error: "+Arrays.toString(value.split("\\|")));
                }
            }
        }).setParallelism(12);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism","12");
        Table tableA = tableEnv.fromDataStream(dynamicSource);

        //Table result = tableA.orderBy($("f0").asc());
        // Table result = tableA.where($("f0").isEqual("12123"));
        Table result = tableA.groupBy($("f1")).select($("f1"),$("f0").max().as("max"));
        try (CloseableIterator<Row> iterator = result.execute().collect()) {
            final Set<Row> actualOutput = new HashSet<>();
            iterator.forEachRemaining(actualOutput::add);
            System.out.println("OutPUT is "+actualOutput.size());
        }

        /*final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "rubber", 2)));

        final DataStream<Order> orderB =
                env.fromCollection(
                        Arrays.asList(
                                new Order(2L, "pen", 3),
                                new Order(2L, "rubber", 3),
                                new Order(4L, "beer", 1)));

        // convert the first DataStream to a Table object
        // it will be used "inline" and is not registered in a catalog
        final Table tableA = tableEnv.fromDataStream(orderA);

        // convert the second DataStream and register it as a view
        // it will be accessible under a name
        tableEnv.createTemporaryView("TableB", orderB);

        // union the two tables
        final Table result =
                tableEnv.sqlQuery(
                        "SELECT * FROM "
                                + tableA
                                + " WHERE amount > 2 UNION ALL "
                                + "SELECT * FROM TableB WHERE amount < 2");

        // convert the Table back to an insert-only DataStream of type `Order`
        tableEnv.toDataStream(result, Order.class).print();

        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();*/
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /** Simple POJO. */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        // for POJO detection in DataStream API
        public Order() {}

        // for structured type detection in Table API
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}
