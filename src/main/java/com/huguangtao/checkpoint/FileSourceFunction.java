package com.huguangtao.checkpoint;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * operator listState
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/8 12:24
 */
public class FileSourceFunction implements SourceFunction<String>, ListCheckpointed<String> {


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
            System.out.println(md5);
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


    // 转换成托管状态
    @Override
    public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {


//        System.out.println("snapshotState");
        return Arrays.asList(new String[]{md5});
    }

    // 用来恢复 和 初始化
    @Override
    public void restoreState(List<String> state) throws Exception {
//        ListStateDescriptor<String> lsd = new ListStateDescriptor<String>("md5",String.class);

        md5 = state.get(0);
    }

}
