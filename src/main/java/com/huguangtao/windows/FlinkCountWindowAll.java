package com.huguangtao.windows;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/11 23:02
 */
public class FlinkCountWindowAll {
    public static void main(String[] args) throws Exception {

        List<String> list = new ArrayList<String>();
        list.add("hainiu\t1");
        list.add("hainiu\t2");
        list.add("hainiu\t3");
        list.add("hainiu\t4");
        list.add("hainiu\t5");
        list.add("hainiu\t6");
        list.add("hainiu\t7");
        list.add("hainiu\t8");
        list.add("hainiu\t9");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.fromCollection(list);
        dss.countWindowAll(2).process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                StringBuilder sb = new StringBuilder();
                for (String s : elements) {
                    sb.append(s);
                }
                out.collect(sb.toString());
            }
        }).print();
        env.execute();

    }
}
