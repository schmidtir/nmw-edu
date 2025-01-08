package com.atguigu.edu.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwdUserUserLoginBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
//        jsonObjectDs.print("JOB");

        KeyedStream<JSONObject, String> keyByDs = jsonObjectDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (element, ts) -> element.getLong("ts")
                        )
        ).keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                }
        );

//        keyByDs.print("KEYBY");

        SingleOutputStreamOperator<JSONObject> processDs = keyByDs.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> firstLoginDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("first_login_dt", JSONObject.class);

                        // 添加状态存活时间
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1L))
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

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect(firstLoginDtState.value());
                    }
                }

        );
        processDs.print("PD");


        SingleOutputStreamOperator<String> mapDs = processDs.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject jsonObject) throws Exception {
                        JSONObject common = jsonObject.getJSONObject("common");
                        Long ts = jsonObject.getLong("ts");
                        String loginTime = DateFormatUtil.tsToDateTime(ts);
                        String dateId = loginTime.substring(0, 10);

                        DwdUserUserLoginBean dwdUserUserLoginBean = DwdUserUserLoginBean.builder()
                                .userId(common.getString("uid"))
                                .dateId(dateId)
                                .loginTime(loginTime)
                                .channel(common.getString("ch"))
                                .provinceId(common.getString("ar"))
                                .versionCode(common.getString("vc"))
                                .midId(common.getString("mid"))
                                .brand(common.getString("ba"))
                                .model(common.getString("md"))
                                .sourceId(common.getString("sc"))
                                .operatingSystem(common.getString("os"))
                                .ts(ts)
                                .build();
                        return JSON.toJSONString(dwdUserUserLoginBean);
                    }
                }
        );

        mapDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_USER_LOGIN));

    }
}
