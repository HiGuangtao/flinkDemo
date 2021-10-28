package com.huguangtao.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 22:02
 */
public class FlinkTumblingsWindowTest {
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
                return Long.valueOf(element.split("\t")[0]);
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\t")[1];
            }
        })


                /**  方法一：
                 * window以后需要对这个窗口中的数据进行处理，sum aggregate reduce fold ....
                 * window().apply(new WindowFuntion(){}) 特有的  虽然能够接触到所有的数据，但是不能获取上下文信息
                 * window().process(new WindowProcessFuntion(){}) 能够接触到所有的数据,能获取上下文信息
                 */
                //                                  Time size, Time offset 表示第十秒中的第二秒
                .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
  /*              .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        context.window().getStart()
                    }
                })*/
                .apply(new WindowFunction<String, String, String, TimeWindow>() {
                    /**
                     *
                     * @param s  key
                     * @param window  window
                     * @param input 数据集
                     * @param out 输出
                     * @throws Exception
                     */
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {
                        Iterator<String> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            String next = iterator.next();
                            out.collect(s + "\t" + next + "\t" + window.getStart() + "\t" + window.getEnd() + "\t");
                        }

                    }

                }).print();
        env.execute();


    }
}
