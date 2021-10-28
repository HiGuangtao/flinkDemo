package com.huguangtao.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * 数据必须是有序的  升序提取器
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 19:30
 */
public class FlinkAscWaterExtracTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.addSource(new SourceFunction<String>() {
            private boolean flag = true;
            private Integer index = 0;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                while (flag) {

                    ctx.collect(System.currentTimeMillis() + "\thgt\t" + index);
                    index++;
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            // 获取时间戳
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.parseLong(element.split("\t")[0]);
            }
        }).print();
        env.execute();


    }
}
