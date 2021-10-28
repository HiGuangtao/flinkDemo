package com.huguangtao.source1;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * 测试自定义反序列化schema
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 19:55
 */
public class MyDer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        Properties prop = new Properties();
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "hgt_32");
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");

        //测试自定义Schema 实现DeserializationSchema
        FlinkKafkaConsumer010<MyRecord> KafkaConsumer = new FlinkKafkaConsumer010<>("hgt32_flink_event", new MySchema(), prop);

        DataStreamSource<MyRecord> dss = env.addSource(KafkaConsumer);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dss.flatMap(new FlatMapFunction<MyRecord, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(MyRecord value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.getName().split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);
        sum.print();
        //测试json格式

        FlinkKafkaConsumer010<ObjectNode> JsonConsumer = new FlinkKafkaConsumer010<>("hgt32_flink_event", new MyJsonSchema(true), prop);
        env.addSource(JsonConsumer).print();
        env.execute();

    }

    public static class MyJsonSchema extends JSONKeyValueDeserializationSchema {

        public MyJsonSchema(boolean includeMetadata) {
            super(includeMetadata);
        }

    }

    public static class MySchema implements DeserializationSchema<MyRecord> {

        @Override
        public MyRecord deserialize(byte[] message) throws IOException {
            //从kafka获取道德数据是二进制的，将二进制转换成字符串，然后包装在对象中
            return new MyRecord(new String(message));
        }

        @Override
        public boolean isEndOfStream(MyRecord nextElement) {
            return false;
        }

        @Override //返回的类型
        public TypeInformation<MyRecord> getProducedType() {
            return TypeInformation.of(MyRecord.class);
        }
    }

    /**
     * 自定义一个类，将从kafka中读取的数据包装成对象的属性
     */
    public static class MyRecord {

        private String name;

        public MyRecord() {
        }

        public MyRecord(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "MyRecord{" +
                    "name='" + name + '\'' +
                    '}';
        }

    }
}
