package com.atguigu.edu.realtime.dwd.app;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author: WZY
 * Create Date: 2025/1/7
 * Create Time: 下午2:00
 * Vserion : 1.0
 * TODO
 */
public class DwdBaseLogApp extends BaseApp{
    public static void main(String[] args) {
        new DwdBaseLogApp().start(10011 , 3 , "dwd_base_log_app" , Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

    }
}
