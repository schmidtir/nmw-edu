package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsUserLoginWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author: WZY
 * Create Date: 2025/1/10
 * Create Time: 上午8:42
 * Vserion : 1.0
 * TODO
 */
public class DwsUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserLoginWindow().start(10025 , 3 , Constant.DWS_USER_USER_LOGIN_WINDOW , Constant.TOPIC_DWD_USER_LOGIN);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // 按照userId分组聚合
        KeyedStream<String, String> keyedStream = ds.keyBy(
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        return jsonObj.getString("userId");
                    }
                }
        );
        // 统计回流用户和独立用户数
        SingleOutputStreamOperator<DwsUserLoginWindowBean> processStream = keyedStream.process(
                new KeyedProcessFunction<String, String, DwsUserLoginWindowBean>() {
                    ValueState<String> lastLoginDtState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastLoginDtStateDesc = new ValueStateDescriptor<>("last_login_dt_state", String.class);
                        lastLoginDtState = getRuntimeContext().getState(lastLoginDtStateDesc);
                    }

                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, DwsUserLoginWindowBean>.Context ctx, Collector<DwsUserLoginWindowBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        Long ts = jsonObj.getLong("ts");
                        String curDate = DateFormatUtil.tsToDate(ts);
                        String lastLoginDt = lastLoginDtState.value();
                        long uvCount = 0L;
                        long backCount = 0L;
                        if (lastLoginDt == null) {
                            uvCount = 1L;
                            lastLoginDtState.update(curDate);
                        } else {
                            if (lastLoginDt.compareTo(curDate) < 0) {
                                // 一定是独立
                                uvCount = 1L;
                                if (ts - DateFormatUtil.dateTimeToTs(lastLoginDt) > 7 * 24 * 3600 * 1000) {
                                    backCount = 1L;
                                }
                                lastLoginDtState.update(curDate);
                            }
                        }
                        if (uvCount != 0L || backCount != 0L) {
                            out.collect(DwsUserLoginWindowBean.builder()
                                    .backCount(backCount)
                                    .uvCount(uvCount)
                                    .ts(ts)
                                    .build()
                            );
                        }
                    }
                }
        );
        // processStream.print();
        SingleOutputStreamOperator<DwsUserLoginWindowBean> withWatermarks = processStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsUserLoginWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<DwsUserLoginWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsUserLoginWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                )
        );
        // withWatermarks.print();
        SingleOutputStreamOperator<DwsUserLoginWindowBean> reduceStream = withWatermarks.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<DwsUserLoginWindowBean>() {
                    @Override
                    public DwsUserLoginWindowBean reduce(DwsUserLoginWindowBean value1, DwsUserLoginWindowBean value2) throws Exception {
                        value1.setBackCount(value1.getBackCount() + value2.getBackCount());
                        value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<DwsUserLoginWindowBean, DwsUserLoginWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<DwsUserLoginWindowBean, DwsUserLoginWindowBean, TimeWindow>.Context context, Iterable<DwsUserLoginWindowBean> elements, Collector<DwsUserLoginWindowBean> out) throws Exception {
                        DwsUserLoginWindowBean next = elements.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        next.setStt(stt);
                        next.setEdt(edt);
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                }
        );
        reduceStream.print();
        reduceStream.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DORIS_DB_NAME , Constant.DWS_USER_USER_LOGIN_WINDOW)
        );
    }
}
