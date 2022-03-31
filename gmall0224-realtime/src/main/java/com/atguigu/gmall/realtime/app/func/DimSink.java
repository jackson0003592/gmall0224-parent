package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author: Felix
 * Date: 2021/8/5
 * Desc: 将维度侧输出流的数据写到Hbase中(通过Phoenix操作)
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //{"database":"gmall0224","data":{"tm_name":"liangliang","id":13},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1628134550}
        //获取维度表表名
        String tableName = jsonObj.getString("sink_table");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //拼接插入语句   upsert into 表空间.表 (a,b,c) values(xx,xx,xx);
        String upsertSql = genUpsertSql(tableName, dataJsonObj);

        System.out.println("向Phoenix维度表中插入数据的SQL:" + upsertSql);

        PreparedStatement ps = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(upsertSql);
            //执行SQL
            ps.executeUpdate();
            //注意：Phoenix的连接实现类不是自动提交事务，需要手动提交
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("向Phoenix维度表中插入数据失败");
        } finally {
            //释放资源
            if (ps != null) {
                ps.close();
            }
        }
        //如果当前维度数据做的是删除或者修改操作
        if (jsonObj.getString("type").equals("update") || jsonObj.getString("type").equals("delete")) {
            //那么清空Redis中缓存的维度数据
            DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
        }

    }

    //拼接插入语句
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        //id ... 100
        //tm_name ... zs
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " (" + StringUtils.join(dataJsonObj.keySet(), ",") + ") " +
                " values('" + StringUtils.join(dataJsonObj.values(), "','") + "')";
        return upsertSql;
    }
}
