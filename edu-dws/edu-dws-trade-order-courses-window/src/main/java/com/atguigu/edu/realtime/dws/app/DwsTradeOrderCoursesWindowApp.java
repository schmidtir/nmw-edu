package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：17:12
 * TODO 交易域课程粒度下单各窗口汇总表
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTradeOrderCoursesWindowApp extends BaseApp {
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

    }
}
