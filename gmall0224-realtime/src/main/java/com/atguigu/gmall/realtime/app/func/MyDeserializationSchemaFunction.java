package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct =(Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");
        String database = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");

        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("database", database);
        jsonObject.put("table", table);
        jsonObject.put("type", type);

        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject dataJsonObj = new JSONObject();
        if(afterStruct != null){
            for (Field field : afterStruct.schema().fields()) {
                String fieleName = field.name();
                Object fieleValue = afterStruct.get(field);
                dataJsonObj.put(fieleName, fieleValue);
            }
        }

        jsonObject.put("data", dataJsonObj);
        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
