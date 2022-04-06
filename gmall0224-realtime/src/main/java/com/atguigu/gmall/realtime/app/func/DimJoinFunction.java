package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {

    String getKey(T obj);

    void join(T obj, JSONObject jsonObj) throws ParseException, Exception;
}
