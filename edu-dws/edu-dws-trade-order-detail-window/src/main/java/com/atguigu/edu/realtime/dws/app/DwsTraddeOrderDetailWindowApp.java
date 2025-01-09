package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：14:40
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeOrderDetailBean;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTraddeOrderDetailWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTraddeOrderDetailWindowApp()
                .start(10028, 3, "Dws_Trade_Order_Detail_Window_App", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print();
        // {
        //     "id": "22316",
        //         "course_id": "93",
        //         "course_name": "CSS3特效实战",
        //         "order_id": "21094",
        //         "user_id": "1144",
        //         "origin_amount": "200.0",
        //         "coupon_reduce": "0.0",
        //         "final_amount": "200.0",
        //         "create_time": "2025-01-06 17:23:31",
        //         "create_date": "2025-01-06",
        //         "out_trade_no": "489696115359374",
        //         "trade_body": "CSS3特效实战等2件商品",
        //         "session_id": "477354c0-febe-43a0-8172-93b9714d1fe8",
        //         "province_id": "17",
        //         "source_id": null,
        //         "ts": 1736155410
        // }

        // 过滤null数据转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDs = ds.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                        try {
                            if (value != null && !value.equals("")) {
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                out.collect(jsonObject);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("");
                        }
                    }
                }
        );

        // jsonObjDs.print("INPUT");

        // 按照 user_id 分组
        SingleOutputStreamOperator<TradeOrderDetailBean> beanDs = jsonObjDs.keyBy(
                        v -> v.getString("user_id")
                )
                // 使用状态判断是否为独立用户 或 新用户
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradeOrderDetailBean>() {

                            private ValueState<String> lastOrderDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastOrderDateDesc =
                                        new ValueStateDescriptor<>("lastOrderDate", String.class);

                                lastOrderDateState = getRuntimeContext().getState(lastOrderDateDesc);
                            }

                            @Override
                            public void processElement(JSONObject value
                                    , KeyedProcessFunction<String, JSONObject, TradeOrderDetailBean>.Context ctx
                                    , Collector<TradeOrderDetailBean> out) throws Exception {

                                String lastOrderDate = lastOrderDateState.value();
                                long ts = value.getLong("ts") * 1000L;
                                String currentOrderDate = DateFormatUtil.tsToDate(ts);

                                if ( !currentOrderDate.equals(lastOrderDate) ) {
                                    // 今日第一次下单（相等则是今日多次下单，不做记录）

                                    TradeOrderDetailBean tradeOrderDetailBean =
                                            TradeOrderDetailBean.builder()
                                                    .stt("")
                                                    .edt("")
                                                    .curDate("")
                                                    .orderUvCount(1L)
                                                    .newOrderUserCount(lastOrderDate == null ? 1L : 0L)
                                                    .ts(ts)
                                                    .build();
                                    out.collect(tradeOrderDetailBean);

                                    lastOrderDateState.update(currentOrderDate);
                                }

                            }
                        }
                );

        // beanDs.print("🫛🫛");

        // 添加水位线
        // 开窗聚合
        SingleOutputStreamOperator<TradeOrderDetailBean> windowDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeOrderDetailBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        ).windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<TradeOrderDetailBean>() {
                    @Override
                    public TradeOrderDetailBean reduce(TradeOrderDetailBean value1, TradeOrderDetailBean value2) throws Exception {
                        value1.setOrderUvCount(value1.getOrderUvCount() + value2.getOrderUvCount());
                        value1.setNewOrderUserCount(value1.getNewOrderUserCount() + value2.getNewOrderUserCount());
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TradeOrderDetailBean, TradeOrderDetailBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderDetailBean, TradeOrderDetailBean, TimeWindow>.Context context
                            , Iterable<TradeOrderDetailBean> elements
                            , Collector<TradeOrderDetailBean> out) throws Exception {

                        TradeOrderDetailBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);

                    }
                }
        );

        // windowDs.print("🪟🪟");

        windowDs.map( new DorisMapFunction<>() )
                .sinkTo( FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRADE_ORDER_DETAIL_WINDOW ) );

    }
}
