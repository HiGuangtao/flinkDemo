package com.huguangtao.checkpoint;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/10 20:25
 */
public class WordCountSumMapState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 6666);

    }
}
