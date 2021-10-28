package com.huguangtao.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * min minby max maxby
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/7 20:45
 */
public class FlinkAggregateMaxMinSum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);


        KeyedStream<Tuple2<String, Integer>, String> ds1 = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(" ");
                return Tuple2.of(split[0], Integer.valueOf(split[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0.substring(0, 1);
            }
        });

//        ds1.maxBy(1).print();
        ds1.min(1).print("min");
        ds1.minBy(1).print("minBY");
        env.execute();

    }
}
