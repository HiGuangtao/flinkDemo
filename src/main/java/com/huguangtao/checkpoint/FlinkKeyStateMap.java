package com.huguangtao.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 * 测试 keyed 的mapStatus
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/9 20:07
 */
public class FlinkKeyStateMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1、重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );
        // 2.checkpoint
        //TODO 设置成1000报错了，改成5000通过
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        DataStreamSource<String> hdfsDss = env.addSource(new FileSourceFunction());
        KeyedStream<Tuple2<String, String>, String> hdfsStream = hdfsDss.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split("\t");
                return Tuple2.of(split[0], split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });

        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "hgt_32");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("hgt32_flink_event", new SimpleStringSchema(), pro);

        DataStreamSource<String> kafkaDss = env.addSource(kafkaConsumer);
        KeyedStream<String, String> kafkaStream = kafkaDss.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        SingleOutputStreamOperator<String> res = hdfsStream.connect(kafkaStream).process(new KeyedCoProcessFunction<String, Tuple2<String, String>, String, String>() {

            MapState<String, String> mapState = null;

            //用于初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                //对keyedStatus 设置超时策略
                StateTtlConfig ttlConfig = StateTtlConfig
                        //keyState的超时时间为100秒
                        .newBuilder(Time.seconds(100))
                        //当创建和更新时，重新计时超时时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //失败时不返回keyState的值
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        //失败时返回keyState的值
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                        .cleanupFullSnapshot()
                        //ttl的时间处理等级目前只支持ProcessingTime
                        .setTimeCharacteristic(StateTtlConfig.TimeCharacteristic.ProcessingTime)
                        .build();
                MapStateDescriptor<String, String> mapStateDesc = new MapStateDescriptor<>("mapStateDesc", String.class, String.class);

                mapStateDesc.enableTimeToLive(ttlConfig);

                //获取mapstate
                mapState = getRuntimeContext().getMapState(mapStateDesc);

            }

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {

                mapState.put(value.f0, value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                String s = mapState.get(value);
                out.collect(s == null ? "no match" : s);

            }
        });
        res.print();
        env.execute();


    }
}
