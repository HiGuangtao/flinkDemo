package com.huguangtao.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 实现将socket流中的数据写入到kafka中
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/7 22:49
 */
public class FlinkKafka01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties pro = new Properties();
        pro.put(ProducerConfig.ACKS_CONFIG, "-1");
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "s3.hadoop:9092,s4.hadoop:9092");
        pro.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);

        //"hgt32_flink_event"
        /*
         * 需要的变量
         * String topicId,
         * KeyedSerializationSchema<T> serializationSchema,
         * Properties producerConfig,
         * @Nullable FlinkKafkaPartitioner<T> customPartitioner
         */
        //原生的producer发送数据的时候
        //Producer.send(ProducerReccord)-->producerRecord
        //k,v,topic,partition 可以指定
        //k.v,topic 按照k的hashcode%partitions数量取余
        //v,topic 自动生成一个随机的key值然后按照hashcode处理，这个key值不断递增
        FlinkKafkaProducer010<Tuple2<String, Integer>> kafkaproducer = new FlinkKafkaProducer010<>(
                "hgt32_flink_event",
                new KeyedSerializationSchema<Tuple2<String, Integer>>() {
                    @Override //指定key
                    public byte[] serializeKey(Tuple2<String, Integer> element) {
                        return element.f0.getBytes(StandardCharsets.UTF_8);
                    }

                    @Override  //指定value
                    public byte[] serializeValue(Tuple2<String, Integer> element) {
                        return (element.f0 + "--" + element.f1).getBytes(StandardCharsets.UTF_8);
                    }

                    @Override
                    public String getTargetTopic(Tuple2<String, Integer> element) {
                        return null;
                    }
                }, pro, new FlinkKafkaPartitioner<Tuple2<String, Integer>>() {
            @Override  //自定义分区
            public int partition(Tuple2<String, Integer> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return record.hashCode() & Integer.MAX_VALUE % partitions.length;
            }
        }
        );

        DataStreamSource<String> socketDss = env.socketTextStream("localhost", 6666);
        socketDss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(Tuple2.of(s1, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).addSink(kafkaproducer);

        env.execute();
    }
}
