package com.huguangtao.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/7 21:39
 */
public class FlinkSinkToHDFS {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("localhost", 6666);
        dss.addSink(new MyHdfsSink("hdfs://ns1/user/qingniu/flink_hdfs_sink"));

        env.execute();
    }

    /**
     * 目标是插入文件到hdfs中，如果没有就创建一个新的，如果存在就拼接
     */
    public static class MyHdfsSink extends RichSinkFunction<String> {
        /**
         * 如果使用构造器那么必须要注意，他是在driver端执行的，不要使用非序列化的对象
         *
         * @param parameters
         * @throws Exception
         */
        private String hdfsPath;
        private Path hPath;

        public MyHdfsSink(String hdfsPath) {
            this.hdfsPath = hdfsPath;
        }

        private FileSystem fs = null;
        private SimpleDateFormat df;

        @Override
        public void open(Configuration parameters) throws Exception {
            df = new SimpleDateFormat("yyyyMMddHHmm");
            hPath = new Path(this.hdfsPath);
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            fs = FileSystem.get(new URI("hdfs://ns1"), conf, "qingniu");
        }

        @Override
        public void close() throws Exception {
            fs.close();
        }

        /**
         * 首先传递进来一个根目录
         * 在根目录下面创建的文件是按照日期和线程的index为标识的
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            String dateStr = df.format(new Date());
            String allPath = this.hdfsPath + "/" + dateStr + "_" + indexOfThisSubtask;
            Path realPath = new Path(allPath);
            FSDataOutputStream fsos = null;
            if (fs.exists(realPath)) {
                fsos = this.fs.append(realPath);
            } else {
                fsos = fs.create(realPath);
            }
            fsos.writeUTF(value);
            fsos.flush();
            fsos.close();
        }
    }
}
