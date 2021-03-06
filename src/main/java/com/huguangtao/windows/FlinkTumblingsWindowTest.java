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


                /**  ????????????
                 * window??????????????????????????????????????????????????????sum aggregate reduce fold ....
                 * window().apply(new WindowFuntion(){}) ?????????  ????????????????????????????????????????????????????????????????????????
                 * window().process(new WindowProcessFuntion(){}) ??????????????????????????????,????????????????????????
                 */
                //                                  Time size, Time offset ??????????????????????????????
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
                     * @param input ?????????
                     * @param out ??????
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
