package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.beans.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: Felix
 * Date: 2021/8/14
 * Desc:  地区主题统计---SQL
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.3 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关设置（略）

        //TODO 3.从指定数据源(kafka)读取数据  转换为动态表
        String topic = "dwm_order_wide";
        String groupId = "province_stats_app_group";

        tableEnv.executeSql("CREATE TABLE order_wide (" +
            " province_id BIGINT, " +
            " province_name STRING," +
            " province_area_code STRING," +
            " province_iso_code STRING," +
            " province_3166_2_code STRING," +
            " order_id STRING," +
            " split_total_amount DOUBLE," +
            " create_time STRING," +
            " rowtime as TO_TIMESTAMP(create_time) ," +
            " WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND " +
            ") WITH (" + MyKafkaUtil.getKafkaDDL(topic,groupId) +")");

        //TODO 4.分组、开窗、聚合计算
        Table provinceStatTable = tableEnv.sqlQuery("select " +
            " DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
            " DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
            " province_id," +
            " province_name," +
            " province_area_code area_code," +
            " province_iso_code iso_code ," +
            " province_3166_2_code iso_3166_2," +
            " count(distinct order_id) order_count," +
            " sum(split_total_amount) order_amount," +
            " UNIX_TIMESTAMP() * 1000 as ts " +
            " from " +
            " order_wide " +
            " group by " +
            " TUMBLE(rowtime, INTERVAL '10' SECOND)," +
            " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code");

        //TODO 5.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStatTable, ProvinceStats.class);

        provinceStatsDS.print(">>>>");

        //TODO 6.将流中的数据写到ClickHouse中
        provinceStatsDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into  province_stats_0224  values(?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();


    }
}
