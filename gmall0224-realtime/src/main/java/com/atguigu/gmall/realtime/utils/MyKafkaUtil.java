package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Author: Felix
 * Date: 2021/7/30
 * Desc: 操作kafka的工具类
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "node2:9092,node3:9092,node4:9092";
    private static final String DEFAULT_TOPIC = "default_topic";

    //获取kafka的消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String str, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, str.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //获取kafka的生产者
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka'," +
                "'topic' = '" + topic + "'," +
                "'properties.bootstrap.servers' = '" + KAFKA_SERVER + "'," +
                "'properties.group.id' = '" + groupId + "'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'";
        return ddl;
    }
}
