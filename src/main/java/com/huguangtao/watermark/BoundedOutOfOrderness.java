package com.huguangtao.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * BoundedOutOfOrdernessTimestampExtractor 用于处理延时时间
 * 该方法是flink自动将两个方法实现了：
 *  getCurrentWatermark
 *  extractTimestamp
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 19:42
 */
public class BoundedOutOfOrderness {
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
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2000L)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split("\t")[0]);
            }
        }).print();
        env.execute();


    }
}
