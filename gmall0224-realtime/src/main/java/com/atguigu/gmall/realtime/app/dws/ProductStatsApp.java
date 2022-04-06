package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.GmallConstant;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.beans.ProductStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: Felix
 * Date: 2021/8/13
 * Desc:  商品主题统计dws
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.检查点相关的配置(略)

        //TODO 3.从kafka主题中读取数据
        //3.1 声明消费主题以及消费者组
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaSource(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce  = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic,groupId);

        //3.3 读取数据  封装为流
        DataStreamSource<String> pageViewStrDS = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoStrDS = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> cartInfoStrDS= env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoStrDS= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoStrDS= env.addSource(commentInfoSource);
        DataStreamSource<String> orderWideStrDS= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideStrDS= env.addSource(paymentWideSource);

        //TODO 4.对流中的数据进行类型的转换  jsonStr->ProductStats
        //4.1 转换点击以及曝光流数据
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pageViewStrDS.process(
            new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = jsonObj.getLong("ts");
                    //判断是否为点击行为
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    String pageId = pageJsonObj.getString("page_id");
                    if (pageId.equals("good_detail")) {
                        //如果当前日志记录的页面是商品的详情页，那么认为这条日志  记录的是点击行为
                        Long skuId = pageJsonObj.getLong("item");
                        ProductStats productStats = ProductStats.builder()
                            .sku_id(skuId)
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                        out.collect(productStats);
                    }

                    //判断是否为曝光
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //如果displays数组不为空，说明当前页面上有曝光行为，对所有的曝光进行遍历
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //判断曝光的是不是商品
                            if (displayJsonObj.getString("item_type").equals("sku_id")) {
                                Long skuId = displayJsonObj.getLong("item");
                                ProductStats productStats1 = ProductStats.builder()
                                    .sku_id(skuId)
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build();
                                out.collect(productStats1);
                            }
                        }
                    }
                }
            }
        );

        //4.2 转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                        .build();
                    return productStats;
                }
            }
        );

        //4.3 转换加购流数据
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                        .build();
                    return productStats;
                }
            }
        );

        //4.4 转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundInfoStatsDS = refundInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);

                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                        .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                        .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                        .build();
                    return productStats;
                }
            }
        );

        //4.5 转换评价流数据
        SingleOutputStreamOperator<ProductStats> commentInfoStatsDS = commentInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                        .build();
                    return productStats;
                }
            }
        );

        //4.6 转换订单宽表流数据
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .build();
                    return productStats;
                }
            }
        );

        //4.7 转换支付宽表流数据
        SingleOutputStreamOperator<ProductStats> paymentWideStatsDS = paymentWideStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                                        PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(DateTimeUtil.toTs(paymentWide.getCallback_time()))
                        .build();
                    return productStats;
                }
            }
        );

        //TODO 5.将不同的流的数据通过union合并到一起
        DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
            favorInfoStatsDS,
            cartInfoStatsDS,
            refundInfoStatsDS,
            commentInfoStatsDS,
            orderWideStatsDS,
            paymentWideStatsDS
        );

        //unionDS.print(">>>>>>>>");

        //TODO 6.指定Watermark以及提起事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<ProductStats>() {
                        @Override
                        public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                            return productStats.getTs();
                        }
                    }
                )
        );

        //TODO 7.分组   注意：目前我们商品维度数据处理订单和支付宽表能够获取，其它的流式没有维度数据。所有我们这里使用skuId进行分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id);

        //TODO 8.开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9.聚合计算
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
            new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                    return stats1;

                }
            },
            new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                @Override
                public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                    for (ProductStats productStats : elements) {
                        productStats.setStt(DateTimeUtil.toYMDHMS(new Date(context.window().getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDHMS(new Date(context.window().getEnd())));
                        productStats.setTs(new Date().getTime());
                        out.collect(productStats);
                    }
                }
            }
        );

        //TODO 10. 补全商品的维度信息
        //10.1 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
            reduceDS,
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                @Override
                public void join(ProductStats productStats, JSONObject dimJsonObj) throws Exception {
                    productStats.setSku_name(dimJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(dimJsonObj.getBigDecimal("PRICE"));
                    productStats.setSpu_id(dimJsonObj.getLong("SPU_ID"));
                    productStats.setCategory3_id(dimJsonObj.getLong("CATEGORY3_ID"));
                    productStats.setTm_id(dimJsonObj.getLong("TM_ID"));
                }

                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }
            },
            60, TimeUnit.SECONDS
        );

        //10.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
            AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);


        //10.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
            AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setCategory3_name(jsonObject.getString("NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //10.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
            AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);


        productStatsWithTmDS.print(">>>>>");

        //TODO 11. 将计算结果写到ClickHouse
        productStatsWithTmDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into product_stats_0224 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
