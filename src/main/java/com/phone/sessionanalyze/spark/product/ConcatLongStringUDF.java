package com.phone.sessionanalyze.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 将两个字段使用指定的分隔符拼接起来
 * UDF3<Long, String, String, String>中的几个类型分别代表：
 * 前两个类型是指调用者传进来的拼接的字段
 * 第三个类型是指用于拼接分隔符
 * 第四个类型是指返回类型
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return String.valueOf(v1) + split + v2;
    }
}
