package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.beans.GmallConstant;
import com.atguigu.gmall.realtime.beans.KeywordStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: Felix
 * Date: 2021/8/14
 * Desc: 关键词统计DWS
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.3 设置并行度
        env.setParallelism(4);

        //TODO 2.注册自定义的UDTF函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.从kafka中读取数据  创建动态表
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_app_group";
        tableEnv.executeSql("CREATE TABLE page_view (" +
            " common MAP<STRING, STRING>, " +
            " page MAP<STRING, STRING>," +
            " ts BIGINT," +
            " rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
            " WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
            ") WITH (" + MyKafkaUtil.getKafkaDDL(topic,groupId) + ")");

        //TODO 4.将动态表中表示搜索行为的记录过滤出来
        Table fullwordTable = tableEnv.sqlQuery("select " +
            " page['item'] fullword,rowtime " +
            " from " +
            " page_view " +
            " where " +
            " page['page_id'] = 'good_list' and page['item'] is not null");

        //TODO 5.使用自定义的UDTF函数 对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery("SELECT rowtime, keyword FROM " + fullwordTable + ", LATERAL TABLE(ik_analyze(fullword)) AS T(keyword)");

        //TODO 6.分组、开窗、聚合计算
        Table resTable = tableEnv.sqlQuery("select " +
            " DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
            " DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
            " keyword," +
            " count(*) ct," +
            " '" + GmallConstant.KEYWORD_SEARCH + "' source," +
            " UNIX_TIMESTAMP() * 1000 as ts " +
            " from " + keywordTable + " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword");

        //TODO 7.将表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(resTable, KeywordStats.class);

        keywordStatsDS.print(">>>>");

        //TODO 8.将流中的数据写到CK中
        keywordStatsDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into keyword_stats_0224(keyword,ct,source,stt,edt,ts)  values(?,?,?,?,?,?)")
        );
        env.execute();
    }
}
