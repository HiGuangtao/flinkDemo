package com.huguangtao.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/9 20:55
 */
public class FlinkbroadcastStateTest {
    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> broadcastDesc = new MapStateDescriptor<>("broadcastState", String.class, String.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        DataStreamSource<String> ds = env.addSource(new FileSourceFunction());

        SingleOutputStreamOperator<Tuple2<String, String>> ds1 = ds.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                String[] split = value.split("\t");
                out.collect(Tuple2.of(split[0], split[1]));

            }
        });

        //将要处理的数据变成一个广播流
        BroadcastStream<Tuple2<String, String>> broadcastStream = ds1.broadcast(broadcastDesc);

        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 6666);
        ds2.connect(broadcastStream).process(new BroadcastProcessFunction<String, Tuple2<String, String>, String>() {
            // socket逻辑
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDesc);
                String s = broadcastState.get(value);
                out.collect(s == null ? "not match" : s);
            }

            //广播状态 逻辑
            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDesc);
                broadcastState.put(value.f0, value.f1);

                for (Map.Entry<String, String> entry : broadcastState.entries()) {
                    String key = entry.getKey();
                    String value1 = entry.getValue();
                    System.out.println(key + "====" + value1);
                    out.collect(key + "--->" + value1);
                }

            }
        }).print();
        env.execute();


    }

}

