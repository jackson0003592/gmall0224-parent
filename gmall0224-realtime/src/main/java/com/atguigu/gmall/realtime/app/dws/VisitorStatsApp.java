package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/8/11
 * Desc: 访客主题统计dws
 * 测试流程
 *  -需要启动的进程
 *      zk、kafka、【hdfs】、logger.sh、ClickHouse
 *      BaseLogApp、UniqueVisitorApp、UserJumpDetailApp、VisitorStatApp
 *  -
 *
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.从Kafka中读取数据
        //2.1 声明读取的主题以及消费者组
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visitor";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        //2.2 获取kafka消费者
        FlinkKafkaConsumer<String> pvSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> ujdSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        //2.3 读取数据 封装流
        DataStreamSource<String> pvStrDS = env.addSource(pvSource);
        DataStreamSource<String> uvStrDS = env.addSource(uvSource);
        DataStreamSource<String> ujdStrDS = env.addSource(ujdSource);

        //pvStrDS.print(">>>>");
        //uvStrDS.print("###");
        //ujdStrDS.print("$$$$");

        //TODO 3.对流中的数据进行类型的转换  jsonStr->VisitorStats
        //3.1 dwd_page_log流中数据的转换
        SingleOutputStreamOperator<VisitorStats> pvStatDS = pvStrDS.map(
            new MapFunction<String, VisitorStats>() {
                @Override
                public VisitorStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        pageJsonObj.getLong("during_time"),
                        jsonObj.getLong("ts")
                    );
                    //判断是否为新的会话
                    String lastPageId = pageJsonObj.getString("last_page_id");
                    if (lastPageId == null || lastPageId.length() == 0) {
                        visitorStats.setSv_ct(1L);
                    }
                    return visitorStats;
                }
            }
        );
        //3.2 dwm_unique_visitor流中数据的转换
        SingleOutputStreamOperator<VisitorStats> uvStatDS = uvStrDS.map(
            new MapFunction<String, VisitorStats>() {
                @Override
                public VisitorStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObj.getLong("ts")
                    );
                    return visitorStats;
                }
            }
        );

        //3.3 dwm_user_jump_detail流中数据的转换
        SingleOutputStreamOperator<VisitorStats> ujdStatDS = ujdStrDS.map(
            new MapFunction<String, VisitorStats>() {
                @Override
                public VisitorStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                    );
                    return visitorStats;
                }
            }
        );

        //TODO 4.将三条流转换后的数据进行合并
        DataStream<VisitorStats> unionDS = pvStatDS.union(uvStatDS, ujdStatDS);

        //unionDS.print(">>>>>>");

        //TODO 5.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<VisitorStats>() {
                        @Override
                        public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                            return visitorStats.getTs();
                        }
                    }
                )
        );

        //TODO 6.按照维度对流中的数据进行分组   维度有：版本、渠道、地区、新老访客  所有我们定义分组的key为Tuple4类型
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(
            new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                    return Tuple4.of(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                    );
                }
            }
        );

        //TODO 7.开窗    对分组之后的数据 进行开窗处理    每个分组 独立的窗口，分组之间互不影响
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 8.聚合计算   对窗口中的数据进行聚合计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
            new ReduceFunction<VisitorStats>() {
                @Override
                public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                    //度量值进行两两相加
                    stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                    stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                    stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                    stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                    stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                    return stats1;
                }
            },
            new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                @Override
                public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                    //补全时间字段
                    for (VisitorStats visitorStats : elements) {
                        visitorStats.setStt(DateTimeUtil.toYMDHMS(new Date(context.window().getStart())));
                        visitorStats.setEdt(DateTimeUtil.toYMDHMS(new Date(context.window().getEnd())));
                        visitorStats.setTs(System.currentTimeMillis());
                        //将处理之后的数据发送的下游
                        out.collect(visitorStats);
                    }
                }
            }
        );

        reduceDS.print(">>>>>>>");

        //TODO 9.将聚合统计之后的数据写到ClickHouse
        reduceDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into visitor_stats_0224 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
