package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.beans.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author: Felix
 * Date: 2021/8/11
 * Desc: 操作ClickHouse的工具类
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql){
        //insert into visitor_stats_0224 values(?,?,?,?,?,?,?,?,?,?,?,?)
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
            sql,
            new JdbcStatementBuilder<T>() {
                //获取流中对象obj的属性值，赋值给问号占位符?    参数 T obj :就是流中一条数据
                @Override
                public void accept(PreparedStatement ps, T obj) throws SQLException {
                    //获取流中对象所属类的属性
                    Field[] fields = obj.getClass().getDeclaredFields();
                    //对属性数组 进行遍历
                    int skipNum = 0;
                    for (int i = 0; i < fields.length; i++) {
                        //获取每一个属性对象
                        Field field = fields[i];

                        //判断该属性是否有@TransientSink注解修饰
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        if(transientSink != null){
                            skipNum ++;
                            continue;
                        }

                        //设置私有属性的访问权限
                        field.setAccessible(true);

                        try {
                            //获取对象的属性值
                            Object fieldValue = field.get(obj);
                            //将属性的值  赋值给问号占位符
                            ps.setObject(i + 1 - skipNum ,fieldValue);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .withUrl(GmallConfig.CLICKHOUSE_URL)
                .build()
        );
        return sinkFunction;
    }
}
