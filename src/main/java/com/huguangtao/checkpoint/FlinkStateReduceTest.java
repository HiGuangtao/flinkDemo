package com.huguangtao.checkpoint;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/9 20:50
 */
public class FlinkStateReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);

        ds.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).process(new KeyedProcessFunction<String, String, String>() {

            ReducingStateDescriptor<String> rdesc = new ReducingStateDescriptor<String>("rdesc", new ReduceFunction<String>() {
                @Override
                public String reduce(String value1, String value2) throws Exception {
                    return value1+"\t"+value2;
                }
            },String.class);
            ReducingState<String> reducingState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
//                valueState = getRuntimeContext().getState(vdesc);
                reducingState = getRuntimeContext().getReducingState(rdesc);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
//             valueState.update(valueState.value()==null?"":valueState.value()+"\t"+value);
//             out.collect(valueState.value());

                reducingState.add(value);
                out.collect(reducingState.get());
            }
        }).print();

        env.execute();
    }

}
