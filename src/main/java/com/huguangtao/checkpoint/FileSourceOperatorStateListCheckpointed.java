package com.huguangtao.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/8 12:22
 */
public class FileSourceOperatorStateListCheckpointed {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(20000L);
        //如果ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        //job在执行CANCE的时候是否删除ck数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //恢复策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        // number of restart attempts
                        3,
                        // delay
                        Time.of(0, TimeUnit.SECONDS)
                )
        );


        DataStreamSource<String> hdfsDss = env.addSource(new FileSourceFunction());
        hdfsDss.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                if (value.contains("中国")) {
                    int a = 1 / 0;
                }
                return value;
            }
        }).print();
        env.execute();

    }
}
