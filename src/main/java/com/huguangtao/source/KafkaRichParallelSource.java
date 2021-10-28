package com.huguangtao.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 需要创建一个kafka生产者来测试
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 17:07
 */
public class KafkaRichParallelSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置属性
        Properties kafkaConsumerProps = new Properties();
        //s2.hadoop:9092 不写
        kafkaConsumerProps.setProperty("bootstrap.servers", "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092");
        kafkaConsumerProps.setProperty("group.id", "hgtflink");
        //30s  分区动态发现，当kafka添加新分区时，无需重启程序就可以发现
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");

        //从kafka获取到的数据源 反序列化的是字符串
/*        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>("hgt32_flink_event", new SimpleStringSchema(), kafkaConsumerProps);

        //设置消费模式，默认是 setStartFromGroupOffsets()
        // kafkaSource.setStartFromLatest();

        kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaInPut = env.addSource(kafkaSource);
        kafkaInPut.print();
        env.execute();*/

        //现在测试自定义的反序列化schema
        FlinkKafkaConsumer010<HainiuKafkaRecord> myKafkaConsumer = new FlinkKafkaConsumer010<>("hgt32_flink_event", new HainiuKafkaRecordSchema(), kafkaConsumerProps);
        DataStreamSource<HainiuKafkaRecord> mySource = env.addSource(myKafkaConsumer);
        mySource.print();
        env.execute();


    }
}
