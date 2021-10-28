package com.huguangtao.source1;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 自定义MyBean的反序列化
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 21:37
 */
public class MyBeanDer implements DeserializationSchema<MyBean> {

    @Override
    public MyBean deserialize(byte[] message) throws IOException {
        return new MyBean(new String(message));
    }

    @Override
    public boolean isEndOfStream(MyBean nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MyBean> getProducedType() {
        return TypeInformation.of(MyBean.class);
    }
}
