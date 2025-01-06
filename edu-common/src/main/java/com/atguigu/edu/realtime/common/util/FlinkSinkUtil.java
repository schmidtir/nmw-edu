package com.atguigu.edu.realtime.common.util;

/* *
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：17:00
 * TODO     Flink Sink 的工具类
 * <p>
 * version: 0.0.1.0
 */


import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class FlinkSinkUtil {

    public static String getDorisSinkDDL( String database , String table) {

        return " WITH ( \n" +
                "       'connector' = 'doris'  \n" +
                "     , 'fenodes' = '" + DORIS_FENODES +"'  \n" +
                "     , 'table.identifier' = '" + database + "." + table +"' \n" +
                "     , 'username' = '" + DORIS_USERNAME + "'  \n" +
                "     , 'password' = '" + DORIS_PASSWORD + "'  \n" +
                "     , 'sink.enable-2pc' = 'false'  \n" +
                " ) ";
    }

    public static String getUpersertKafkaSinkDDL( String topic){
        return  " WITH ( \n " +
                " 'connector' = 'upsert-kafka' " +
                " , 'topic' = '" + topic + "' " +
                " , 'properties.bootstrap.servers' = '" + KAFKA_BROKERS +  "' " +
                " , 'key.format' = 'json' " +
                " , 'value.format' = 'json' " +
                " ) ";
    }

    public static String getKafkaSinkDDL( String topic){
        return  " WITH ( \n " +
                " 'connector' = 'kafka' " +
                " , 'topic' = '" + topic + "' " +
                " , 'properties.bootstrap.servers' = '" + KAFKA_BROKERS +  "' " +
                " , 'format' = 'json' " +
                " , 'sink.delivery-guarantee' = 'at-least-once' " +
//                         " , 'sink.delivery-guarantee' = 'exactly-once' " +

                " ) ";
    }

    public static String getKafkaSourceDDL(String topic,String groupId) {

        return "  WITH ( \n " +
                " 'connector' = 'kafka' " +
                " , 'topic' = '" + topic + "' " +
                " , 'properties.bootstrap.servers' = '" + KAFKA_BROKERS +  "' " +
                " , 'properties.group.id' = '"+ groupId + "' " +
                " , 'scan.startup.mode' = 'earliest-offset' " +
//                " , 'scan.startup.mode' = 'latest-offset' " +
                " , 'format' = 'json' " +
                " , 'json.ignore-parse-errors' = 'true' " +
                // " , 'isolation.level' = 'read_committed' " +
                " ) ";
    }
}
