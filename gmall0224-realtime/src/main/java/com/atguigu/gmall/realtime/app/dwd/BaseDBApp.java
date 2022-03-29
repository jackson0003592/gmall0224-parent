package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(""));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(j ->
                j.getString("table") != null
                        && j.getString("table").length() > 0
                        && j.getJSONObject("data") != null
                        && j.getString("data").length() > 3
        );

        DebeziumSourceFunction<String> mySQLSoueceFunction = MySQLSource.<String>builder()
                .hostname("node2")
                .port(3306)
                .databaseList("gmall0224_realtime")
                .tableList("gmall0224_realtime.table_rpocess")
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchemaFunction())
                .build();

        DataStreamSource<String> mySQLDS = env.addSource(mySQLSoueceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<>("table_process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mySQLDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(new TableProcessFunction(dimTag, mapStateDescriptor));

        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);
        realDS.print(">>>");
        dimDS.print("###");

        dimDS.addSink(new DimSink());

        realDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(topic, jsonObject.getJSONObject("data").toJSONString().getBytes());
            }
        }));
    }
}
