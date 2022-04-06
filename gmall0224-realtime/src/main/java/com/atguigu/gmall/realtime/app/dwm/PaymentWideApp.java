package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentInfo;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点设置 (略)

        //TODO 3.从Kafka中读取数据
        //3.1 声明消费主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_app_group";

        //3.2 获取消费者对象
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);

        //3.3 读取数据  封装流
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoSource);
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideSource);

        //TODO 4.对数据类型进行转换  String-> 实体对象
        //支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));
        //订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        //paymentInfoDS.print(">>>>");
        //orderWideDS.print("#####");

        //TODO 5.指定Watermark并提取事件时间字段
        //支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );

        //订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );

        //TODO 6.按照订单id进行分区     指定关联字段
        //支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        //订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWatermarkDS.keyBy(OrderWide::getOrder_id);


        //TODO 7.支付和订单宽表的双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        paymentWideDS.print(">>>");

        //TODO 8.将支付宽表数据写到kafka的dwm_payment_wide
        paymentWideDS
                .map(paymentWide->JSON.toJSONString(paymentWide))
                .addSink(
                        MyKafkaUtil.getKafkaSink("dwm_payment_wide")
                );

        env.execute();
    }
}
