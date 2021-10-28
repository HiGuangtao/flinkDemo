package com.huguangtao.watermark;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * watermark测试
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/10 21:14
 */
public class FlinkTestWaterMark1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//默认200ms，修改成2s
//        env.getConfig().setAutoWatermarkInterval(2000L);

        //source端要想实现发送watermark，需要手动指定
        env.addSource(new SourceFunction<String>() {
            private Boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (flag) {
                    long currentTimeMillis = System.currentTimeMillis();
                    String word = "hgt\t" + currentTimeMillis;
                    ctx.collect(word);

                    //产生watermark方式一：表示在源就带上watermark
//                    ctx.emitWatermark(new Watermark(currentTimeMillis - 3000));

                    Thread.sleep(1000);

                }
            }

            @Override
            public void cancel() {
                this.flag = false;
            }
        })/*//方式二：在datasource后调用assignTimestampsAndWatermarks方法    间接性的
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private Long current_timeStamp = 0L;
                    private Long autoforderness = 3000L;

                    //系统间歇性的获取时间(watermark)
                    //200ms系统提取当前的watermark

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        System.out.println("getCurrentWatermark");
                        return new Watermark(this.current_timeStamp - autoforderness);
                    }

                    @Override //提取数据上面的事件时间,每个数据都提取
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        System.out.println("extractTimestamp");
                        current_timeStamp = Long.parseLong(element.split("\t")[1]);
                        return 0;
                    }
                })*/
                //方式三：确定性地，来一个问一个
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {

                    private Long autoforderness = 3000L;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                        System.out.println("checkAndGetNextWatermark");
                        return new Watermark(extractedTimestamp - autoforderness);
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        System.out.println("extractTimestamp");
                        return Long.parseLong(element.split("\t")[1]);
                    }
                })
                //方法四：AscendingTimestampExtractor 适用于event时间戳单调递增的场景，用于有序数据流
/*                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String element) {
                        System.out.println("extractAscendingTimestamp  " + Long.parseLong(element.split("\t")[1]));

                        return Long.parseLong(element.split("\t")[1]);
                    }
                })*/
                .print();
        env.execute();


    }
}
