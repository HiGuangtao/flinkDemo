package com.huguangtao.source;

/**
 *
 * 自定义schema  测试
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/26 16:52
 */
public class HainiuKafkaRecord {
    private String record;

    public HainiuKafkaRecord(String record) {
        this.record = record;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return "HainiuKafkaRecord{" +
                "record='" + record + '\'' +
                '}';
    }
}
