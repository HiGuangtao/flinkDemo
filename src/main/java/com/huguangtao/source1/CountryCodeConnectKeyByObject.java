package com.huguangtao.source1;

import com.huguangtao.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 21:18
 */
public class CountryCodeConnectKeyByObject {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties pro = new Properties();
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "hgt_32");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");

        //从kafka中获取数据 包装成了对象
        FlinkKafkaConsumer010<MyBean> KafkaSource = new FlinkKafkaConsumer010<>("hgt32_flink_event", new MyBeanDer(), pro);
        DataStreamSource<MyBean> kafkaDss = env.addSource(KafkaSource);
        //用String作为key
        KeyedStream<MyBean, String> kafkaKeyedStream = kafkaDss.keyBy(new KeySelector<MyBean, String>() {
            @Override
            public String getKey(MyBean value) throws Exception {
                return value.getName();
            }
        });

        //从textfile中获取数据
        DataStreamSource<String> textDss = env.addSource(new FileCountryDictSourceFunction());
        KeyedStream<Tuple2<String, String>, String> textKeyedStream = textDss.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] words = value.split("\t");
                return Tuple2.of(words[0], words[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });


        //两个流connect 并且两个流数据类型不同
        kafkaKeyedStream.connect(textKeyedStream).process(new KeyedCoProcessFunction<String, MyBean, Tuple2<String, String>, String>() {
             HashMap<String, String> map = new HashMap<>();

            @Override //从kafka中获取的数据
            public void processElement1(MyBean value, Context ctx, Collector<String> out) throws Exception {
                String s = map.get(value.getName());
                String result = s == null ? "not match" : s;
                out.collect(result);
            }

            @Override // text data
            public void processElement2(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0, value.f1);
                out.collect(value.toString());
            }
        }).print();
        env.execute();


    }
}
