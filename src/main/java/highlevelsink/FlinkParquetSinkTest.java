package highlevelsink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * 我们要写入的是parquet格式的数据
 * 列式存储的数据都包含数据和Schema
 *
 */
public class FlinkParquetSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(20000L);
        //如果ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        //job在执行CANCE的时候是否删除ck数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //指定保存ck的存储模式，这个是默认的
        MemoryStateBackend stateBackend = new MemoryStateBackend(10 * 1024 * 1024, false);
        env.setStateBackend(stateBackend);

        //恢复策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );

        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);
        DateTimeBucketAssigner<ParquetVo> parquetVoDateTimeBucketAssigner = new DateTimeBucketAssigner<>("yyyy/MMdd/HH", ZoneId.of("Asia/Shanghai"));
        StreamingFileSink<ParquetVo> sink = StreamingFileSink.forBulkFormat(
                new Path("file:///E://tmp/parquet_sink"),
                MyParquetAvroWriters.forReflectRecord(ParquetVo.class,CompressionCodecName.SNAPPY) )
                .withBucketAssigner(parquetVoDateTimeBucketAssigner)
                .withBucketCheckInterval(10000)
                .build();
//      parquet.gzip --> parquet.snappy
        ds.map(new MapFunction<String, ParquetVo>() {
            @Override
            public ParquetVo map(String value) throws Exception {
                return new ParquetVo(value,20);
            }
        }).addSink(sink);

        env.execute();
    }


    public static class MyParquetAvroWriters{


        /**
         * Creates a ParquetWriterFactory for an Avro specific type. The Parquet writers will use the
         * schema of that specific type to build and write the columnar data.
         *
         * @param type The class of the type to write.
         */
        public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(Class<T> type,CompressionCodecName code) {
            final String schemaString = SpecificData.get().getSchema(type).toString();
            final ParquetBuilder<T> builder = (out) -> createAvroParquetWriter(schemaString, SpecificData.get(), out,code);
            return new ParquetWriterFactory<>(builder);
        }

        /**
         * Creates a ParquetWriterFactory that accepts and writes Avro generic types.
         * The Parquet writers will use the given schema to build and write the columnar data.
         *
         * @param schema The schema of the generic type.
         */
        public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema,CompressionCodecName code) {
            final String schemaString = schema.toString();
            final ParquetBuilder<GenericRecord> builder = (out) -> createAvroParquetWriter(schemaString, GenericData.get(), out,code);
            return new ParquetWriterFactory<>(builder);
        }

        /**
         * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro
         * to reflectively create a schema for the type and use that schema to write the columnar data.
         *
         * @param type The class of the type to write.
         */
        public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type,CompressionCodecName code) {
            final String schemaString = ReflectData.get().getSchema(type).toString();
            final ParquetBuilder<T> builder = (out) -> createAvroParquetWriter(schemaString, ReflectData.get(), out,code);
            return new ParquetWriterFactory<>(builder);
        }

        private static <T> ParquetWriter<T> createAvroParquetWriter(
                String schemaString,
                GenericData dataModel,
                OutputFile out,
                CompressionCodecName code
        ) throws IOException {

            final Schema schema = new Schema.Parser().parse(schemaString);

            return AvroParquetWriter.<T>builder(out)
                    .withSchema(schema)
                    .withDataModel(dataModel)
                    .withCompressionCodec(code)
                    .build();
        }

    }


    public static class ParquetVo{
        String name;
        int age;
        public ParquetVo() {

        }
        public ParquetVo(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
