package highlevelsink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;
/*
hadoop支持的文件格式
文本文件，没有压缩
而且是不是列存储的
 */
public class FlinkHadoopSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);
        BucketingSink<String> bucketingSink = new BucketingSink<>("file:///tmp/hadoop_sink");
        //alter table xxx add partition (year=2021,day=0912,hour=14)
        bucketingSink.setBucketer(new DateTimeBucketer<String>("yyyy/MMdd/HH",ZoneId.of("Asia/Shanghai")));
        bucketingSink.setBatchSize(10*1024);
        bucketingSink.setBatchRolloverInterval(10000);
        bucketingSink.setInProgressPrefix(".");
        bucketingSink.setPartSuffix(".log");
        bucketingSink.setPartPrefix("hainiu");

        ds.addSink(bucketingSink);

        env.execute();
    }
}
