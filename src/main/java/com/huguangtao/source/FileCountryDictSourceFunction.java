package com.huguangtao.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * 自定义数据源，需要实现sourceFunction（非并行的）
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 14:51
 */
public class FileCountryDictSourceFunction implements SourceFunction<String> {
    //如果是ParallelSourceFunction，则该文件会被读取n遍，n是cpu核数
//public class FileCountryDictSourceFunction implements ParallelSourceFunction<String> {

    private Boolean isCancel = true;
    private Integer interval = 5000;
    private String md5 = null;


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
//        Path path = new Path("/user/hgt32/flink/country_data/country_dict.txt");
        Path path = new Path("/user/hgt32/flink/a.txt");

        //hadoop 配置
        Configuration conf = new Configuration();

        //hdfs客户端
        FileSystem fs = FileSystem.get(conf);

        while (isCancel) {
            //如果不存在，跳出此次循环
            if (!fs.exists(path)) {
                Thread.sleep(interval);
                System.out.println("不存在");
                continue;
            }
            FileChecksum fileChecksum = fs.getFileChecksum(path);
            String md5Str = fileChecksum.toString();

            //
            if (!md5Str.equals(md5)) {
                FSDataInputStream open = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    ctx.collect(line);
                }
                reader.close();
                open.close();
                md5 = md5Str;
            }
            Thread.sleep(interval);
        }
    }
    @Override
    public void cancel() {
        isCancel = false;
    }


}
