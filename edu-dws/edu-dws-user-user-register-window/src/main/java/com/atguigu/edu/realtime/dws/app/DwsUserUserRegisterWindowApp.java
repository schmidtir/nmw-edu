package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsUserRegisterWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 用户域用户注册各窗口汇总表
 */
public class DwsUserUserRegisterWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindowApp().start(10026,3,"dws_user_user_register_window_app",Constant.TOPIC_DWD_USER_REGISTER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.清洗、转换
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> beanDs = ds.flatMap(
                new FlatMapFunction<String, DwsUserRegisterWindowBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwsUserRegisterWindowBean> out) throws Exception {
                        if (value != null) {
                            JSONObject jsonObject = JSON.parseObject(value);
                            long ts = jsonObject.getLong("ts") * 1000;
                            out.collect(DwsUserRegisterWindowBean.builder()
                                    .registerCount(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
        );

        beanDs.print("BD");

        //2.分组去重
        beanDs.keyBy


        //3.开窗聚合

        //4.写出到doris
    }
}
