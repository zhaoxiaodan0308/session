package com.phone.sessionanalyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * UDF2<String, String, String>的几个类型指的是：
 * 前两类型是传进来的值的类型
 * 第一个类型是指json格式的字符串，
 * 第二个类型是指要获取json字符串中的字段名
 * 第三个类型是指返回某个字段的值
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    @Override
    public String call(String json, String field) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(json);
        return jsonObject.getString(field);
    }
}
