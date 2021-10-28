package com.huguangtao.source1;

import java.util.Objects;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/9/6 21:21
 */
public class MyBean {
    private String name;

    public MyBean(String name) {
        this.name = name;
    }

    public MyBean() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "MyBean{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
