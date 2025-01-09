package com.atguigu.edu.realtime.common.util;

import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

/**
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author: WZY
 * Create Date: 2025/1/6
 * Create Time: 下午4:57
 * Vserion : 1.0
 * TODO
 */
public class FlinkSqlUtil {

    public static void createTopicDb(StreamTableEnvironment  tableEnv , String groupId ) {
        tableEnv.executeSql("create table topic_db (" +
                " `database` string ,\n" +
                " `table` string ,\n" +
                " `type` string ,\n" +
                " `data` map<string,string>,\n" +
                "`ts` string\n" +
                ")" + getKafkaSourceDDL(TOPIC_DB , groupId)
                );
    }

    public static String getDorisSinkDDL( String database , String table) {

        return " WITH ( \n" +
                "       'connector' = 'doris'  \n" +
                "     , 'fenodes' = '" + DORIS_FENODES +"'  \n" +
                "     , 'table.identifier' = '" + database + "." + table +"' \n" +
                "     , 'username' = '" + DORIS_USERNAME + "'  \n" +
                "     , 'password' = '" + DORIS_PASSWORD + "'  \n" +
                "     , 'sink.enable-2pc' = 'false'  \n" +
                // "     ,  'sink.enable-2pc' = 'true'  \n" +
                // "     ,  'sink.label-prefix' = 'doris_label'  " +
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
                // " , 'sink.transaction-id-prefix' = 'gmall-realtime-" + System.currentTimeMillis() + "' " +
                // " , 'transaction.timeout.ms' = '60000' " +
                " ) ";
    }

    public static String getKafkaSourceDDL(String topic,String groupId) {

        return "  WITH ( \n " +
                " 'connector' = 'kafka' " +
                " , 'topic' = '" + topic + "' " +
                " , 'properties.bootstrap.servers' = '" + KAFKA_BROKERS +  "' " +
                " , 'properties.group.id' = '"+ groupId + "' " +
//                " , 'scan.startup.mode' = 'earliest-offset' " +
                " , 'scan.startup.mode' = 'latest-offset' " +
                " , 'format' = 'json' " +
                " , 'json.ignore-parse-errors' = 'true' " +
                // " , 'isolation.level' = 'read_committed' " +
                " ) ";
    }
}
