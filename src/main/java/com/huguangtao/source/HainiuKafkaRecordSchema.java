package com.huguangtao.source;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 自定义反序列化schema 需要实现 HainiuKafkaRecord 接口
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 16:54
 */
public class HainiuKafkaRecordSchema implements DeserializationSchema<HainiuKafkaRecord> {


    /**
     * @param message 从kafka传过来的二进制数据源
     * @return 返回字符串，然后生成一个实体类对象
     * @throws IOException 不知道
     */
    @Override
    public HainiuKafkaRecord deserialize(byte[] message) throws IOException {
        return new HainiuKafkaRecord(new String(message));
    }

    /**
     * 判断什么时候结束
     * @param nextElement 下一个数据
     * @return false表示不结束
     */
    @Override
    public boolean isEndOfStream(HainiuKafkaRecord nextElement) {
        return false;
    }

    /**
     * 告知返回什么类型
     * @return HainiuKafkaRecord.class
     */
    @Override
    public TypeInformation<HainiuKafkaRecord> getProducedType() {
        return TypeInformation.of(HainiuKafkaRecord.class);
    }
}
