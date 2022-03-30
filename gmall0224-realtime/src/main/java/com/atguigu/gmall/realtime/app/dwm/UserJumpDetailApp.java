package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

//        DataStream<String> kafkaDS = env
//            .fromElements(
//                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"home\"},\"ts\":15000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"detail\"},\"ts\":30000} "
//            );


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                        return jsonObj.getLong("ts");
                    }
                }));

        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return true;
            }
        }).within(Time.seconds(10));

        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {
        };
        //9.2 提取数据
        SingleOutputStreamOperator<String> resDS = patternDS.flatSelect(
                timeoutTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //超时情况  就是我们要统计的跳出
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObj : jsonObjectList) {
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                },
                //处理完全匹配数据的
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //完全匹配的数据  是跳转情况，不在我们的统计范围之内
                    }
                }
        );

        //9.3 从侧输出流中获取超时数据（跳出）
        DataStream<String> jumpDS = resDS.getSideOutput(timeoutTag);

        jumpDS.print(">>>>");

        //TODO 10.将跳出明细写到kafka的dwm层主题
        jumpDS.addSink(
                MyKafkaUtil.getKafkaSink("dwm_user_jump_detail")
        );

        env.execute();
    }
}
