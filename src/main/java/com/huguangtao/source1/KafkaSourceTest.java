package com.huguangtao.source1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 练习从Kafka读取字符串类型数据，反序列化schema是SimpleStringSchema
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 19:32
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        Properties pro = new Properties();
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "hgt_32");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");
        //通过Properties设置读取offset策略earliest：从
//        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        FlinkKafkaConsumer010<String> KafkaSource = new FlinkKafkaConsumer010<>("hgt32_flink_event", new SimpleStringSchema(), pro);

        //通过FlinkKafkaConsumer010 设置读取offset策略：读最新的策略
//        KafkaSource.setStartFromLatest();
//        KafkaSource.setStartFromEarliest();
        //这个是默认的，从提交到kafka中的偏移量中读取offset
        KafkaSource.setStartFromGroupOffsets();
        DataStreamSource<String> dss = env.addSource(KafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        sum.print();
        env.execute();

    }
}
