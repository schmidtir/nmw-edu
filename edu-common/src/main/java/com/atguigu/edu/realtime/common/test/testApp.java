package com.atguigu.edu.realtime.common.test;

/* *
 * Package Name: com.atguigu.edu.realtime.common.test
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：20:20
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.TOPIC_DB;

public class testApp extends BaseApp {
    public static void main(String[] args) {
        new testApp()
                .start(10001,4,"test1", TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        ds.print();
    }
}
