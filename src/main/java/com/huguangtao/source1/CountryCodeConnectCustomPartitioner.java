package com.huguangtao.source1;

import com.huguangtao.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 23:30
 */
public class CountryCodeConnectCustomPartitioner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties pro = new Properties();
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "hgt_32");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");

        FlinkKafkaConsumer010<MyBean> KafkaSource = new FlinkKafkaConsumer010<>("hgt32_flink_event", new MyBeanDer(), pro);
        DataStreamSource<MyBean> kafkadds = env.addSource(KafkaSource);

        SingleOutputStreamOperator<MyBean> map = kafkadds.map(new MapFunction<MyBean, MyBean>() {
            @Override
            public MyBean map(MyBean value) throws Exception {
                String name = value.getName();
                Random random = new Random();
                int i = random.nextInt(8);
                return new MyBean(i + "_" + name);
            }
        });
        DataStream<MyBean> kafkaPartitioned = map.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                String[] s = key.split("_");
                return Integer.parseInt(s[0]);
            }
        }, new KeySelector<MyBean, String>() {
            @Override
            public String getKey(MyBean value) throws Exception {
                return value.getName();
            }
        });

        //text 流
        DataStreamSource<String> textDss = env.addSource(new FileCountryDictSourceFunction());
        SingleOutputStreamOperator<Tuple2<String, String>> flatMap = textDss.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                String[] words = value.split("\t");
                String code = words[0];
                String country = words[1];
                //TODO i = 1; i <=8 =>java.lang.RuntimeException: 8  i = 1; i < 8就可以
                for (int i = 1; i <=8 ; i++) {
                    String resCode = i + "_" + code;
                    out.collect(Tuple2.of(resCode, country));
                }

            }
        });
        DataStream<Tuple2<String, String>> textPartitioned = flatMap.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                String[] s = key.split("_");
                return Integer.parseInt(s[0]);
            }
        }, new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<String> result = textPartitioned.connect(kafkaPartitioned).process(new CoProcessFunction<Tuple2<String, String>, MyBean, String>() {
            Map<String, String> map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0, value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(MyBean value, Context ctx, Collector<String> out) throws Exception {
                String s = map.get(value.getName());
                String result = s == null ? "not match!" : s;
                out.collect(result);
            }
        });
        result.print();
        env.execute();

    }
}
