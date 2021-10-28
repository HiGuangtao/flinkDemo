package com.huguangtao.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 用来熟悉flink的函数
 * <p>
 * function 写法
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 11:36
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> socketStreamSource = env.socketTextStream("localhost", 8888);


        //测试rich flatmap方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple = socketStreamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("==Rich的open方法");
            }

            @Override
            public void close() throws Exception {
                System.out.println("==Rich的close方法");
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                    out.collect(Tuple2.of(word, 1));
                    if ("close".equals(word)){
                        close();
                    }
                }
            }
        });


        //===== FlatMapFunction
        /*SingleOutputStreamOperator<Tuple2<String, Integer>> tuple = socketStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });*/

        /*
         * keyBy 的几种方法：返回KeyedStream
         */
        //方法一：
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream1 = tuple.keyBy(0);

        //方法二：
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream2 = tuple.keyBy("f0");

        //方法三：KeySelector<Tuple2<String, Integer>, String> 中String是key
        KeyedStream<Tuple2<String, Integer>, String> keyByStream3 = tuple.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        /*
         KeyedStream<Tuple2<String, Integer>, String>
         group by 完成后 value聚合
         sum(1) 0 代表key，1代表value
         */
        
        //
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyByStream1.sum(1);
        sum.print();
        env.execute();

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyByStream1.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });






        keyByStream1.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Integer>() {
            int sum = 0;
            MapState<String, Integer> mapState = null;
            @Override
            public void open(Configuration parameters) throws Exception {

                MapStateDescriptor<String, Integer> mapStateDesc = new MapStateDescriptor<>("mapStateDesc", String.class, Integer.class);
                mapState = getRuntimeContext().getMapState(mapStateDesc);

            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out) throws Exception {

                String currentKey = ctx.getCurrentKey().toString();
                sum += value.f1;
                mapState.put(currentKey,sum);
                out.collect(sum);

            }
        })
                .print();


        
        
        



        /*//获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //获取socket流
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 6666);

        //flatMap
        SingleOutputStreamOperator<String> flatMap = socketSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //map: word--> (word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleMap = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        //按照key聚合 group by key
//        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = tupleMap.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = tupleMap.keyBy("f0");
*//*        KeyedStream<Tuple2<String, Integer>, String> keyBy = tupleMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*//*

        //将value求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        //打印输出
        sum.print();
        env.execute();*/


    }
}
