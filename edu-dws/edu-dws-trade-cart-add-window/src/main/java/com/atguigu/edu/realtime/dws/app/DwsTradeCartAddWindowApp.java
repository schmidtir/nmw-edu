package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：8:44
 * TODO 交易域加购各窗口汇总表
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeCartAddWindowBean;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTradeCartAddWindowApp extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeCartAddWindowApp()
                .start(10027,3, "dws_trade_cart_add_window_app_test", "dwd_trade_cart_add" );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print();
        // {
        //     "course_id": 452,
        //         "cart_price": 200.00,
        //         "create_time": "2025-01-06 17:23:28",
        //         "user_id": 246,
        //         "course_name": "大数据技术之Flume1.9",
        //         "session_id": "28af7dd4-9279-4913-991f-ecf788204bb7",
        //         "id": 5696,
        //         "ts": 1736 1554 08
        // }
        // 按照 user_id分组
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> beanDs = ds.map(JSON::parseObject)
                .keyBy(json -> json.getString("user_id"))
                // 使用状态过滤出独立用户数
                .process(
                        new KeyedProcessFunction<String, JSONObject, DwsTradeCartAddWindowBean>() {

                            ValueState<String> lastCartAddDtState;

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                ValueStateDescriptor<String> lastCartAddDtStateDesc =
                                        new ValueStateDescriptor<>("lastCartAddDtState", String.class);

                                lastCartAddDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                lastCartAddDtState = getRuntimeContext().getState(lastCartAddDtStateDesc);

                            }

                            @Override
                            public void processElement(JSONObject value
                                    , KeyedProcessFunction<String, JSONObject, DwsTradeCartAddWindowBean>.Context ctx
                                    , Collector<DwsTradeCartAddWindowBean> out) throws Exception {

                                Long cartAddUvCount = 0L;

                                String lastAddCartDt = lastCartAddDtState.value();

                                long currentAddCartTs = value.getLong("ts") * 1000;
                                String currentAddCartDate = DateFormatUtil.tsToDate(currentAddCartTs);

                                if (!currentAddCartDate.equals(lastAddCartDt)) {

                                    cartAddUvCount = 1L;
                                    lastCartAddDtState.update(currentAddCartDate);
                                }

                                if (cartAddUvCount != 0L) {

                                    DwsTradeCartAddWindowBean dwsTradeCartAddWindowBean
                                            = DwsTradeCartAddWindowBean
                                            .builder()
                                            .stt("")
                                            .edt("")
                                            .curDate("")
                                            .cartAddUvCount(cartAddUvCount)
                                            .ts(currentAddCartTs)
                                            .build();
                                    out.collect(dwsTradeCartAddWindowBean);
                                }
                            }
                        }
                );

        // beanDs.print("🫛🫛");

        // 设置水位线
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> tsAndWaterMarkDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCartAddWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        );

        // 开窗聚合
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> windowDs = tsAndWaterMarkDs.windowAll(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
                new ReduceFunction<DwsTradeCartAddWindowBean>() {

                    @Override
                    public DwsTradeCartAddWindowBean reduce(DwsTradeCartAddWindowBean value1, DwsTradeCartAddWindowBean value2) throws Exception {
                        value1.setCartAddUvCount(value1.getCartAddUvCount() + value2.getCartAddUvCount());
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<DwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<DwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean, TimeWindow>.Context context
                            , Iterable<DwsTradeCartAddWindowBean> elements
                            , Collector<DwsTradeCartAddWindowBean> out) throws Exception {

                        DwsTradeCartAddWindowBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);

                    }
                }
        );
        //windowDs.print("🪟🪟");

        // 数据写入doris
        windowDs.map( new DorisMapFunction<>() )
                .sinkTo( FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRADE_CART_ADD_WINDOW ) );

    }
}
