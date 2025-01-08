package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：17:12
 * TODO 交易域课程粒度下单各窗口汇总表
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeOrderCoursesWindowBean;
import com.atguigu.edu.realtime.common.function.DimRichMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;

public class DwsTradeOrderCoursesWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderCoursesWindowApp()
                .start(10030,3, "dws_trade_order_courses_window_app", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        SingleOutputStreamOperator<JSONObject> filterDs = ds.flatMap(
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

        // 结构转换
        SingleOutputStreamOperator<TradeOrderCoursesWindowBean> beanDs = filterDs.map(
                value -> {
                    String courseId = value.getString("course_id");
                    BigDecimal finalAmount = value.getBigDecimal("final_amount");
                    Long ts = value.getLong("ts") * 1000L;

                    TradeOrderCoursesWindowBean tradeOrderCoursesWindowBean =
                            TradeOrderCoursesWindowBean
                                    .builder()
                                    .stt("")
                                    .edt("")
                                    .curDate("")
                                    .courseId(courseId)
                                    .courseName("")
                                    .subjectId("")
                                    .subjectName("")
                                    .categoryId("")
                                    .categoryName("")
                                    .orderTotalAmount(finalAmount)
                                    .ts(ts)
                                    .build();
                    return tradeOrderCoursesWindowBean;
                }
        );

        // 添加水位线
        SingleOutputStreamOperator<TradeOrderCoursesWindowBean> windowDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeOrderCoursesWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        ).keyBy(
                obj -> obj.getCourseId()
        ).windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<TradeOrderCoursesWindowBean>() {

                    @Override
                    public TradeOrderCoursesWindowBean reduce(TradeOrderCoursesWindowBean value1, TradeOrderCoursesWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TradeOrderCoursesWindowBean, TradeOrderCoursesWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderCoursesWindowBean, TradeOrderCoursesWindowBean, TimeWindow>.Context context
                            , Iterable<TradeOrderCoursesWindowBean> elements
                            , Collector<TradeOrderCoursesWindowBean> out) throws Exception {

                        TradeOrderCoursesWindowBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);

                    }
                }
        );

        // windowDs.print("🪟🪟");

        // 关联维度信息
        windowDs.map(
                new DimRichMapFunction<TradeOrderCoursesWindowBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }

                    @Override
                    public String getRowKey(TradeOrderCoursesWindowBean bean) {
                        return bean.getCourseId();
                    }

                    @Override
                    public void addDim(TradeOrderCoursesWindowBean bean, JSONObject dimJsonObj) {

                        bean.setCourseName(dimJsonObj.getString("course_name"));
                        bean.setSubjectId(dimJsonObj.getString("subject_id"));

                    }
                }
        );

        // cnameDs.print("🧊🧊");


    }
}
