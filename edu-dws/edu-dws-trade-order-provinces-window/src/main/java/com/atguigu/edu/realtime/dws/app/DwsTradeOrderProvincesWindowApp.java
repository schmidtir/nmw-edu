package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：9:41
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeOrderProvincesBean;
import com.atguigu.edu.realtime.common.function.DimRichMapFunction;
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

import java.math.BigDecimal;
import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTradeOrderProvincesWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderProvincesWindowApp()
                .start(10032,3, "dws_trade_order_provinces_window_app", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {


        // ds.print();
        SingleOutputStreamOperator<TradeOrderProvincesBean> filteDs = ds.flatMap(
                new FlatMapFunction<String, TradeOrderProvincesBean>() {
                    @Override
                    public void flatMap(String value, Collector<TradeOrderProvincesBean> out) throws Exception {

                        try {
                            if (
                                    value != null && !value.equals("")
                                            && JSONObject.parseObject(value).getString("user_id") != null
                                            && JSONObject.parseObject(value).getString("province_id") != null
                            ) {
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                String orderId = jsonObject.getString("order_id");
                                String provinceId = jsonObject.getString("province_id");
                                String userId = jsonObject.getString("user_id");
                                BigDecimal finalAmount = jsonObject.getBigDecimal("final_amount");
                                Long ts = jsonObject.getLong("ts") * 1000L;

                                TradeOrderProvincesBean tradeOrderProvincesBean =
                                        TradeOrderProvincesBean
                                                .builder()
                                                .stt("")
                                                .edt("")
                                                .curDate("")
                                                .provinceId(provinceId)
                                                .provinceName("")
                                                .userId(userId)
                                                .orderId(orderId)
                                                .orderTotalAmount(finalAmount)
                                                .orderUuCount(0L)
                                                .orderCount(0L)
                                                .ts(ts)
                                                .build();

                                out.collect( tradeOrderProvincesBean );
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("");
                        }
                    }
                }
        );

        // filteDs.print("🎢🎢");

        SingleOutputStreamOperator<TradeOrderProvincesBean> beanDs = filteDs
                .keyBy(bean -> bean.getOrderId())
                .process(
                        new KeyedProcessFunction<String, TradeOrderProvincesBean, TradeOrderProvincesBean>() {
                            private ValueState<String> lastOrderDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastOrderDateStateDesc =
                                        new ValueStateDescriptor<>("lastOrderDate", String.class);

                                lastOrderDateState = getRuntimeContext().getState(lastOrderDateStateDesc);
                            }

                            @Override
                            public void processElement(TradeOrderProvincesBean value
                                    , KeyedProcessFunction<String, TradeOrderProvincesBean, TradeOrderProvincesBean>.Context ctx
                                    , Collector<TradeOrderProvincesBean> out) throws Exception {

                                long orderCount = 0L;
                                String lastOrderDate = lastOrderDateState.value();
                                Long ts = value.getTs();
                                String curDate = DateFormatUtil.tsToDate(ts);

                                if (!curDate.equals(lastOrderDate)) {
                                    orderCount = 1L;

                                    value.setOrderCount(orderCount);
                                    out.collect(value);

                                    lastOrderDateState.update(curDate);
                                }

                            }
                        }
                ).keyBy(bean -> bean.getUserId() + bean.getProvinceId())
                .process(
                        new KeyedProcessFunction<String, TradeOrderProvincesBean, TradeOrderProvincesBean>() {

                            private ValueState<String> lastPAndUDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastPAndUDateDesc = new ValueStateDescriptor<>("lastPAndUDate", String.class);
                                lastPAndUDateState = getRuntimeContext().getState(lastPAndUDateDesc);
                            }

                            @Override
                            public void processElement(TradeOrderProvincesBean value
                                    , KeyedProcessFunction<String, TradeOrderProvincesBean, TradeOrderProvincesBean>.Context ctx
                                    , Collector<TradeOrderProvincesBean> out) throws Exception {

                                long orderUuCount = 0L;
                                String lastDate = lastPAndUDateState.value();
                                Long ts = value.getTs();
                                String curDate = DateFormatUtil.tsToDate(ts);
                                if (!curDate.equals(lastDate)) {
                                    orderUuCount = 1L;
                                    value.setOrderUuCount(orderUuCount);
                                    out.collect(value);
                                    lastPAndUDateState.update(curDate);
                                }
                            }
                        }
                );

//        beanDs.print("🫛🫛");
        SingleOutputStreamOperator<TradeOrderProvincesBean> windowDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeOrderProvincesBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        ).windowAll(
                TumblingEventTimeWindows.of(Time.seconds(5L))
        ).reduce(
                new ReduceFunction<TradeOrderProvincesBean>() {
                    @Override
                    public TradeOrderProvincesBean reduce(TradeOrderProvincesBean value1, TradeOrderProvincesBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TradeOrderProvincesBean, TradeOrderProvincesBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderProvincesBean, TradeOrderProvincesBean, TimeWindow>.Context context, Iterable<TradeOrderProvincesBean> elements, Collector<TradeOrderProvincesBean> out) throws Exception {
                        TradeOrderProvincesBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);
                    }
                }
        );

        // windowDs.print("🪟🪟");

        SingleOutputStreamOperator<TradeOrderProvincesBean> resultDs = windowDs.map(
                new DimRichMapFunction<TradeOrderProvincesBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeOrderProvincesBean bean) {
                        return bean.getProvinceId();
                    }

                    @Override
                    public void addDim(TradeOrderProvincesBean bean, JSONObject dimJsonObj) {
                        bean.setProvinceName( dimJsonObj.getString("name") );

                    }
                }
        );

        resultDs.print("🍎🍎🍎");

        resultDs.map( new DorisMapFunction<>() )
                .sinkTo(FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRADE_ORDER_PROVINCES_WINDOW));
    }
}
