package com.huguangtao.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 21:36
 */
public class FlinkGlobalWindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dss = env.addSource(new SourceFunction<String>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 1;
                while (flag) {
                    ctx.collect(System.currentTimeMillis() + "\thainiu\t" + num);
//                    if(num %10 == 0)
//                        Thread.sleep(3000);
                    num++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });
        //获取时间戳和watermark
        SingleOutputStreamOperator<String> hasAssigned = dss.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.parseLong(element.split("\t")[0]);
            }
        });
        // keyed
        KeyedStream<String, String> hasKeyed = hasAssigned.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\t")[1];
            }
        });

        hasKeyed.window(GlobalWindows.create())
                //filter
                .evictor(new Evictor<String, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        //过滤之前 在计算前能够拿到所有的数据
                        for (TimestampedValue<String> element : elements) {
                            System.out.println("before:" + element.getValue());
                        }
                        System.out.println("before");

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        System.out.println("end");
                        //过滤之后
                        Iterator<TimestampedValue<String>> it = elements.iterator();
                        //为什么用迭代器遍历不用for？ 主要是想调用remove方法
                        while (it.hasNext()) {
                            TimestampedValue<String> next = it.next();
                            System.out.println(next.getValue());
                            it.remove();
                        }
                        System.out.println("end");
                    }
                }).trigger(CountTrigger.of(3)).process(new ProcessWindowFunction<String, String, String, GlobalWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                int sum = 0;
                for (String element : elements) {
                    System.out.println("window-->" + element + "\t");
                    sum += 1;
                }
                System.out.println("<--window");
                out.collect(sum + "");
            }
        }).print();
        env.execute();

    }
}