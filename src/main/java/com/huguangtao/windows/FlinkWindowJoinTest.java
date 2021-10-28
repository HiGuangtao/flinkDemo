package com.huguangtao.windows;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 23:09
 */
public class FlinkWindowJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 7777);

        KeyedStream<String, String> ds1HasKeyed = ds1.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        KeyedStream<String, String> ds2HasKeyed = ds2.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        JoinedStreams<String, String>.Where<String> hasJoined = ds1HasKeyed.join(ds2HasKeyed).where(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        JoinedStreams<String, String>.Where<String>.EqualTo hasEqualTo = hasJoined.equalTo(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        hasEqualTo.window(GlobalWindows.create()).trigger(CountTrigger.of(1)).apply(new JoinFunction<String, String, String>() {
            @Override
            public String join(String first, String second) throws Exception {
                return first + "\t" + second;
            }
        }).print();
        env.execute();


    }
}
