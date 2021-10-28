package com.huguangtao.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 15:42
 */
public class FileSourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取自定义source
        DataStreamSource<String> fileSource = env.addSource(new FileCountryDictSourceFunction());
        //打印输出
        fileSource.print();
        //执行
        env.execute();

    }
}
