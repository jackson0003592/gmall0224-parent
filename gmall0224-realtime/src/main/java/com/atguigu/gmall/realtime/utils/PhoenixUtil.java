package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {

    private static Connection conn;

    private static void initConn(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clz){
        if(conn == null){
            initConn();
        }

        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()){
                T obj = clz.newInstance();
                for (int i=1; i<= md.getColumnCount(); i++){
                    String columnName = md.getColumnClassName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (rs != null){
                try {
                    rs.close();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }

            if(ps != null){
                try {
                    ps.close();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) {
        List<JSONObject> jsonObjects = queryList("select * from dim_base_trademark", JSONObject.class);
        System.out.println(jsonObjects);
    }
}
