package com.huguangtao.checkpoint;

import com.huguangtao.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 测试重启策略
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/7 19:14
 */
public class reset3Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //重启策略可以在flink-conf.yaml中配置，表示全局的配置。也可以在应用代码中动态指定，会覆盖全局配置
        //固定延迟的代码 3: number of restart attempts
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(0, TimeUnit.SECONDS)));


       /* DataStreamSource<String> dss = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(Tuple2.of(s1, 1));
                    System.out.println(s1);
                    if ("hgt".equals(s1)) {
                        int a = 1 / 0;
                    }
                }
            }
        });*/

        DataStreamSource<String> HdfsSource = env.addSource(new FileCountryDictSourceFunction());
        HdfsSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                if (value.contains("中国")) {
                    //一般设置错的方式
                    int a = 1 / 0;
                }
                return value;
            }
        }).print();
        env.execute();
    }
}
