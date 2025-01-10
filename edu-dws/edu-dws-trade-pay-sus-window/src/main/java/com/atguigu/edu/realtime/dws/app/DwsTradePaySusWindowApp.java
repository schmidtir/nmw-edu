package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：15:56
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradePaySusWindowBean;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTradePaySusWindowApp extends BaseApp {

    public static void main(String[] args) {
        new DwsTradePaySusWindowApp()
                .start( 10029, 3,"dws_trade_pay_sus_window_app", TOPIC_DWD_TRADE_PAY_SUS_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // ds.print();
        // {
        //     "id": "14732",
        //         "out_trade_no": "292647998516396",
        //         "order_id": "27637",
        //         "alipay_trade_no": "a8fe9636-448a-4f64-a558-070bb5f9438c",
        //         "total_amount": "200",
        //         "trade_body": "IDE中快速配置Maven与Git等1件商品",
        //         "payment_type": "1102",
        //         "payment_status": "1602",
        //         "create_time": "2025-01-07 21:13:05",
        //         "create_date": "2025-01-07",
        //         "callback_time": "2025-01-07 21:13:15",
        //         "course_id": null,
        //         "course_name": null,
        //         "user_id": null,
        //         "origin_amount": null,
        //         "coupon_reduce": null,
        //         "final_amount": null,
        //         "session_id": null,
        //         "province_id": null,
        //         "source_id": null,
        //         "ts": 1736255584
        // }

        // 过滤null值转换数据结构
        SingleOutputStreamOperator<JSONObject> filterDs = ds.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            if (value != null && JSONObject.parseObject(value).getString("user_id") != null) {

                                JSONObject obj = JSONObject.parseObject(value);
                                out.collect( obj );

                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(value + "为脏数据");
                        }
                    }
                }
        );

        // filterDs.print("🎢🎢");
        // 按照user_id分组
        SingleOutputStreamOperator<TradePaySusWindowBean> beanDs = filterDs.keyBy(jsonObj -> jsonObj.getString("user_id"))
                // 判断是否为新用户或者当天独立用户
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradePaySusWindowBean>() {

                            private ValueState<String> lastPayDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastPayDateStateDesc =
                                        new ValueStateDescriptor<>("lastPayDate", String.class);

                                lastPayDateState = getRuntimeContext().getState( lastPayDateStateDesc );
                            }

                            @Override
                            public void processElement(JSONObject value
                                    , KeyedProcessFunction<String, JSONObject, TradePaySusWindowBean>.Context ctx
                                    , Collector<TradePaySusWindowBean> out) throws Exception {

                                String lastPayDate = lastPayDateState.value();
                                long ts = value.getLong("ts") * 1000L;
                                String currentPayDate = DateFormatUtil.tsToDate(ts);

                                if (!currentPayDate.equals(lastPayDate)) {
                                    TradePaySusWindowBean tradePaySusWindowBean =
                                            TradePaySusWindowBean
                                                    .builder()
                                                    .stt("")
                                                    .edt("")
                                                    .curDate("")
                                                    .paySucUvCount(1L)
                                                    .paySucNewUserCount(lastPayDate == null ? 1L : 0L)
                                                    .ts(ts)
                                                    .build();

                                    out.collect(tradePaySusWindowBean);
                                    lastPayDateState.update(currentPayDate);
                                }

                            }
                        }
                );

        // beanDs.print("🫛🫛");
        // 设置水位线
        SingleOutputStreamOperator<TradePaySusWindowBean> tsAndWaterMarkDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradePaySusWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        );

        // tsAndWaterMarkDs.print("🎠🎠");

        // 开窗聚合
        SingleOutputStreamOperator<TradePaySusWindowBean> windowDs = tsAndWaterMarkDs.windowAll(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
                new ReduceFunction<TradePaySusWindowBean>() {
                    @Override
                    public TradePaySusWindowBean reduce(TradePaySusWindowBean value1, TradePaySusWindowBean value2) throws Exception {
                        value1.setPaySucNewUserCount(value1.getPaySucNewUserCount() + value2.getPaySucNewUserCount());
                        value1.setPaySucUvCount(value1.getPaySucUvCount() + value2.getPaySucUvCount());
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TradePaySusWindowBean, TradePaySusWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradePaySusWindowBean, TradePaySusWindowBean, TimeWindow>.Context context
                            , Iterable<TradePaySusWindowBean> elements
                            , Collector<TradePaySusWindowBean> out) throws Exception {

                        TradePaySusWindowBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);
                    }
                }
        );

        //windowDs.print("🪟🪟");

        windowDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRADE_PAY_SUS_WINDOW )
        );
    }
}
