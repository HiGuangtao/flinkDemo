package com.huguangtao;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/10/21 12:42
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(Tuple2.of(s1, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = flatMap.keyBy("0");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyed.sum(1);
        sum.print();

        env.execute();

    }
}
