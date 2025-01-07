package com.atguigu.edu.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdUserUserLoginApp extends BaseApp {
    public static void main(String[] args) {
        new DwdUserUserLoginApp()
                .start(10015,3,"dwd_user_user_login_app", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<JSONObject> jsonObjectDs = ds.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (jsonObject.getJSONObject("common").getString("uid") != null) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
        );

        jsonObjectDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (element, ts) -> element.getLong("ts")
                        )
        ).keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return  value.getJSONObject("common").getString("mid");
                    }
                }
        ).process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> firstLoginDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("first_login_dt", JSONObject.class);

                        // 添加状态存活时间
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig
                                .newBuilder(Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

                        firstLoginDtState = getRuntimeContext().getState(valueStateDescriptor);

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 处理数据
                        // 获取状态
                        JSONObject firstLoginDt = firstLoginDtState.value();
                        Long ts = jsonObject.getLong("ts");
                        if (firstLoginDt == null) {
                            firstLoginDtState.update(jsonObject);
                            // 第一条数据到的时候开启定时器
                            ctx.timerService().registerEventTimeTimer(ts + 10 * 1000L);
                        } else {
                            Long lastTs = firstLoginDt.getLong("ts");
                            if (ts < lastTs) {
                                firstLoginDtState.update(jsonObject);
                            }
                        }

                }
        );

    }
}
