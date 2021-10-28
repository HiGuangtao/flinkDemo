package com.huguangtao.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 22:27
 */
public class FlinkSessionWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new SourceFunction<String>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 1;
                while (flag) {
                    ctx.collect(System.currentTimeMillis() + "\thainiu\t" + num);
                    if (num % 10 == 0) {
                        Thread.sleep(6000);
                    }
                    num++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.parseLong(element.split("\t")[0]);
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\t")[1];
            }
        }).window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<String>() {
            @Override
            public long extract(String element) {
                if (Integer.parseInt(element.split("\t")[2]) % 10 == 0) {
                    return 5000L;
                } else {
                    return 2000L;
                }
            }
        })).aggregate(new AggregateFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override //创建新的累加器
            public Tuple2<String, Integer> createAccumulator() {
                return Tuple2.of(" ", 0);
            }

            @Override
            public Tuple2<String, Integer> add(String value, Tuple2<String, Integer> accumulator) {

                accumulator.f1 += Integer.parseInt(value.split("\t")[2]);
                return Tuple2.of(value.split("\t")[1], accumulator.f1);
            }

            @Override
            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                return accumulator;
            }

            @Override
            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                return Tuple2.of(a.f0, a.f1 + b.f1);
            }
        }).print();
        env.execute();


    }
}
