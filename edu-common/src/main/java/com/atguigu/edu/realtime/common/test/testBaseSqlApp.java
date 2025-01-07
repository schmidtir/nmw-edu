package com.atguigu.edu.realtime.common.test;

/* *
 * Package Name: com.atguigu.edu.realtime.common.test
 * Author : Kevin
 * Create Date ：2025/1/7
 * Create Time ：16:34
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class testBaseSqlApp extends BaseSqlApp {
    public static void main(String[] args) throws Exception {
        new testBaseSqlApp()
                .start( 10003,3, "test_base_sql_app");
    }
    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception {

        readOdsTopicDb( streamTableEnv, "test_base_sql_app" );

        streamTableEnv.executeSql(
                "SELECT * FROM topic_db"
        ).print();
    }
}
