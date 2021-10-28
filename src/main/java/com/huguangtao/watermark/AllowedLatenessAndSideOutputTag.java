package com.huguangtao.watermark;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Iterator;

/**
 *
 * 延迟数据处理，用于乱序数据流不在windows中，在别一个地方处理延时数据。
 * allowedLateness()，设定最大延迟时间，触发被延迟，不宜设置太长
 * sideOutputTag，设置侧输出标记，侧输出是可用于给延迟数据设
 * 置标记，然后根据标记再获取延迟的数据 ，这样就不会丢弃数据了
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 19:58
 */
public class AllowedLatenessAndSideOutputTag {

    private static final OutputTag<String> lateOutputTag = new OutputTag<>("late", BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<String> streamOperator = env.addSource(new SourceFunction<String>() {
            private boolean flag = true;
            private Integer index = 1;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (flag) {

                    Long currentTime = System.currentTimeMillis();

                    if (index % 5 == 0) {
                        currentTime -= 5000L;
                    }
                    String s = currentTime + "\thgt\t" + index;
                    Thread.sleep(1000);
                    ctx.collect(s);
                    index++;
                }

            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.parseLong(element.split("\t")[0]);

            }
        });

        KeyedStream<String, String> keyBy = streamOperator.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        WindowedStream<String, String, TimeWindow> late = keyBy.timeWindow(Time.seconds(2)).allowedLateness(Time.seconds(1))
                .sideOutputLateData(lateOutputTag);

        SingleOutputStreamOperator<String> process = late.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                System.out.println("subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                        ",start:" + context.window().getStart() +
                        ",end:" + context.window().getEnd() +
                        ",waterMarks:" + context.currentWatermark() +
                        ",currentTime:" + System.currentTimeMillis());

                Iterator<String> iterator = elements.iterator();
                for (; iterator.hasNext(); ) {
                    String next = iterator.next();
                    System.out.println("windows-->" + next);
                    out.collect("on time:" + next);
                }
            }
        });
        //输出正常的
        process.print();
        //输出延迟的 侧输出流
        process.getSideOutput(lateOutputTag).print("late:");
        env.execute();

    }
}
