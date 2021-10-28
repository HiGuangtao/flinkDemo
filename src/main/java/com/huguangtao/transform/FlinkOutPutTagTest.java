package com.huguangtao.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * 侧输出流 拆分流
 * 从socket输入多个手机号码，符合要求178开头的正常输出，脏数据侧输出
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/7 20:51
 */
public class FlinkOutPutTagTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 6666);

        //BasicTypeInfo.STRING_TYPE_INFO 这个是通过SimpleStringSchema源码发现的
        OutputTag<String> invalid1 = new OutputTag<>("invalid", BasicTypeInfo.STRING_TYPE_INFO);
        //
        OutputTag<String> invalid = new OutputTag<>("invalid", TypeInformation.of(String.class));

        //判断输入是否符合条件：11位 138开头
        SingleOutputStreamOperator<String> process = socketSource.process(new ProcessFunction<String, String>() {
            List<String> list = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                list.add("138");
                list.add("178");
                list.add("137");
            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                if (value.length() == 11 && list.contains(value.substring(0, 3))) {
                    out.collect(value);
                } else {

                    ctx.output(invalid, value);
                }
            }
        });
        process.print();
        process.getSideOutput(invalid).print("invalid");
        env.execute();
    }
}
