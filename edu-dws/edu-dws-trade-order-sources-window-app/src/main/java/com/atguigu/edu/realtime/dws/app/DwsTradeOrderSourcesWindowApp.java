package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：19:21
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeOrderSourcesBean;
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

public class DwsTradeOrderSourcesWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderSourcesWindowApp()
                .start(10031,3,"dws_trade_order_sources_window_app",TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {


        // ds.print();
        SingleOutputStreamOperator<TradeOrderSourcesBean> filteDs = ds.flatMap(
                new FlatMapFunction<String, TradeOrderSourcesBean>() {
                    @Override
                    public void flatMap(String value, Collector<TradeOrderSourcesBean> out) throws Exception {

                        try {
                            if (
                                    value != null && !value.equals("")
                                    && JSONObject.parseObject(value).getString("user_id") != null
                                    && JSONObject.parseObject(value).getString("source_id") != null
                            ) {
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                String orderId = jsonObject.getString("order_id");
                                String sourceId = jsonObject.getString("source_id");
                                String userId = jsonObject.getString("user_id");
                                BigDecimal finalAmount = jsonObject.getBigDecimal("final_amount");
                                Long ts = jsonObject.getLong("ts") * 1000L;

                                TradeOrderSourcesBean tradeOrderSourcesBean =
                                        TradeOrderSourcesBean
                                                .builder()
                                                .stt("")
                                                .edt("")
                                                .curDate("")
                                                .sourceId(sourceId)
                                                .userId(userId)
                                                .sourceName("")
                                                .orderId(orderId)
                                                .orderTotalAmount(finalAmount)
                                                .orderUuCount(0L)
                                                .orderCount(0L)
                                                .ts(ts)
                                                .build();

                                out.collect(tradeOrderSourcesBean);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("");
                        }
                    }
                }
        );

        // filteDs.print("🎢🎢");
        // TradeOrderSourcesBean(
        //          stt=,
        //          edt=,
        //          curDate=,
        //          sourceId=2,
        //          userId=579,
        //          sourceName=,
        //          orderId=25310,
        //          orderTotalAmount=200.0,
        //          orderUuCount=0,
        //          orderCount=0,
        //          ts=1736250113000)

        SingleOutputStreamOperator<TradeOrderSourcesBean> beanDs = filteDs
                .keyBy(bean -> bean.getOrderId())
                .process(
                        new KeyedProcessFunction<String, TradeOrderSourcesBean, TradeOrderSourcesBean>() {

                            private ValueState<String> lastOrderDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastOrderDateStateDesc =
                                        new ValueStateDescriptor<>("lastOrderDate", String.class);

                                lastOrderDateState = getRuntimeContext().getState(lastOrderDateStateDesc);
                            }

                            @Override
                            public void processElement(TradeOrderSourcesBean value
                                    , KeyedProcessFunction<String, TradeOrderSourcesBean, TradeOrderSourcesBean>.Context ctx
                                    , Collector<TradeOrderSourcesBean> out) throws Exception {

                                long orderCount = 0L;
                                String lastOrderDate = lastOrderDateState.value();
                                Long ts = value.getTs();
                                String curDate = DateFormatUtil.tsToDate(ts);

                                if (curDate != lastOrderDate) {
                                    orderCount = 1L;
                                    value.setOrderCount(orderCount);
                                    out.collect(value);
                                    lastOrderDateState.update(curDate);
                                }


                            }
                        }
                ).keyBy(
                        bean -> bean.getUserId() + bean.getSourceId()
                ).process(
                        new KeyedProcessFunction<String, TradeOrderSourcesBean, TradeOrderSourcesBean>() {
                            private ValueState<String> lastSourceUserDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastSourceUserDateDesc =
                                        new ValueStateDescriptor<>("lastSourceUserDate", String.class);
                                lastSourceUserDateState = getRuntimeContext().getState(lastSourceUserDateDesc);
                            }

                            @Override
                            public void processElement(TradeOrderSourcesBean value
                                    , KeyedProcessFunction<String, TradeOrderSourcesBean, TradeOrderSourcesBean>.Context ctx
                                    , Collector<TradeOrderSourcesBean> out) throws Exception {
                                long orderUuCount = 0L;
                                String lastSUDate = lastSourceUserDateState.value();
                                Long ts = value.getTs();
                                String curDate = DateFormatUtil.tsToDate(ts);
                                if (curDate != lastSUDate) {
                                    orderUuCount = 1L;
                                    value.setOrderUuCount(orderUuCount);
                                    out.collect(value);
                                    lastSourceUserDateState.update(curDate);
                                }



                            }
                        }
                );

        // beanDs.print("🫛🫛");
        SingleOutputStreamOperator<TradeOrderSourcesBean> windowDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeOrderSourcesBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        ).windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(
                new ReduceFunction<TradeOrderSourcesBean>() {
                    @Override
                    public TradeOrderSourcesBean reduce(TradeOrderSourcesBean value1, TradeOrderSourcesBean value2) throws Exception {
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TradeOrderSourcesBean, TradeOrderSourcesBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderSourcesBean, TradeOrderSourcesBean, TimeWindow>.Context context, Iterable<TradeOrderSourcesBean> elements, Collector<TradeOrderSourcesBean> out) throws Exception {
                        TradeOrderSourcesBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);
                    }
                }
        );

        // windowDs.print("🪟🪟");
        // 🪟🪟:2> TradeOrderSourcesBean(
        //          stt=2025-01-07 21:10:50
        //          , edt=2025-01-07 21:11:00
        //          , curDate=2025-01-07
        //          , sourceId=2
        //          , userId=152
        //          , sourceName=
        //          , orderId=27116
        //          , orderTotalAmount=800.0
        //          , orderUuCount=4, orderCount=4, ts=1736255451000)

        SingleOutputStreamOperator<TradeOrderSourcesBean> resultDs = windowDs.map(
                new DimRichMapFunction<TradeOrderSourcesBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_source";
                    }

                    @Override
                    public String getRowKey(TradeOrderSourcesBean bean) {
                        return bean.getSourceId();
                    }

                    @Override
                    public void addDim(TradeOrderSourcesBean bean, JSONObject dimJsonObj) {
                        bean.setSourceName(dimJsonObj.getString("source_site"));

                    }
                }
        );

        resultDs.print("🍎🍎🍎");


        resultDs.map( new DorisMapFunction<>() )
                .sinkTo(FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRADE_ORDER_SOURCES_WINDOW));
    }
}
