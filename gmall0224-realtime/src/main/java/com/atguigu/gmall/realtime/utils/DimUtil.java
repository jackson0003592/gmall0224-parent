package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Author: Felix
 * Date: 2021/8/9
 * Desc:  查询维度数据的工具类
 */
public class DimUtil {

    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("ID", id));
    }

    //使用旁路缓存  对维度查询进行优化
    //Redis缓存：      type: String           key:  dim:表名:主键值1_主键值2     ttl:1天
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValues) {
        //拼接查询维度SQL
        StringBuilder selectDimSql = new StringBuilder("select * from " + tableName + " where ");
        //拼接查询Redis的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");

        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;

            selectDimSql.append(colName + "='" + colValue + "'");
            redisKey.append(colValue);

            if (i < colNameAndValues.length - 1) {
                selectDimSql.append(" and ");
                redisKey.append("_");
            }
        }

        //先根据key到Redis中查询缓存的维度数据
        //声明操作Redis的客户端
        Jedis jedis = null;
        //声明变量  用于接收从Redis中查询出的缓存数据
        String jsonStr = null;
        //声明变量  用于处理返回的维度对象
        JSONObject dimInfoJsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            //从Redis中获取维度数据
            jsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("从Redis中查询维度数据发生了异常");
        }

        //判断是否从Redis中获取到了维度缓存数据
        if (jsonStr != null && jsonStr.length() > 0) {
            //从Redis中查到了维度的缓存数据，将缓存的维度字符串转换为json对象
            dimInfoJsonObj = JSON.parseObject(jsonStr);
        } else {
            //从Redis中没有查到维度的缓存数据， 发送请求到Phoenix库中去查询
            System.out.println("查询维度的SQL:" + selectDimSql);
            //底层还是调用我们封装的查询Phoenix表数据的方法
            List<JSONObject> dimList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);
            if (dimList != null && dimList.size() > 0) {
                //注意：因为是根据维度数据的主键去查询，所有只会返回一条数据
                dimInfoJsonObj = dimList.get(0);
                //将从Phoenix中查询出来的维度数据  写到Redis缓存中
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 3600 * 24, dimInfoJsonObj.toJSONString());
                }

            } else {
                System.out.println("维度数据没找到:" + selectDimSql);
            }
        }


        if (jedis != null) {
            jedis.close();
            System.out.println("----关闭Redis连接----");
        }

        return dimInfoJsonObj;
    }

    //从Phoenix表中查询维度数据   {"ID":"12","TM_NAME":"AAA"}
    //"select * from dim_base_trademark where id = '12' and TM_NAME ='AAA'", JSONObject.class
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValues) {
        //拼接查询维度SQL
        StringBuilder selectDimSql = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;

            selectDimSql.append(colName + "='" + colValue + "'");
            if (i < colNameAndValues.length - 1) {
                selectDimSql.append(" and ");
            }
        }
        System.out.println("查询维度的SQL:" + selectDimSql);

        //底层还是调用我们封装的查询Phoenix表数据的方法
        List<JSONObject> dimList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);
        JSONObject dimInfoJsonObj = null;
        if (dimList != null && dimList.size() > 0) {
            //注意：因为是根据维度数据的主键去查询，所有只会返回一条数据
            dimInfoJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据没找到:" + selectDimSql);
        }
        return dimInfoJsonObj;
    }

    public static void main(String[] args) {
        JSONObject dimInfo = DimUtil.getDimInfoNoCache("dim_base_trademark", Tuple2.of("ID", "12"));
        //JSONObject dimInfo = DimUtil.getDimInfo("dim_base_trademark", Tuple2.of("ID", "12"));
//        JSONObject dimInfo = DimUtil.getDimInfo("dim_base_trademark", "12");
        System.out.println(dimInfo);
    }

    //根据Redis的key  删除Redis中记录
    public static void deleteCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("删除Redis缓存发生了异常");
        }
    }
}
