package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：16:38
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TrafficPageViewBean;
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

public class DwsTrafficPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficPageViewWindowApp()
                .start(10023,3,"dws_traffic_page_view_window_app", TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print();
        // {
        //     "common": {
        //             "sc": "2",
        //             "ar": "20",
        //             "uid": "751",
        //             "os": "iOS 13.3.1",
        //             "ch": "Appstore",
        //             "is_new": "0",
        //             "md": "iPhone Xs Max",
        //             "mid": "mid_314",
        //             "vc": "v2.1.132",
        //             "ba": "iPhone",
        //             "sid": "84b9060a-1d48-43a6-a7d6-f27571a5bb76"
        // },
        //     "page": {
        //     "page_id": "course_list",
        //             "item": "数据库",
        //             "during_time": 17962,
        //             "item_type": "keyword",
        //             "last_page_id": "home"
        // },
        //     "ts": 1736 1554 11183
        // }

        SingleOutputStreamOperator<TrafficPageViewBean> filterDs = ds.flatMap(
                new FlatMapFunction<String, TrafficPageViewBean>() {
                    @Override
                    public void flatMap(String value, Collector<TrafficPageViewBean> out) throws Exception {

                        try {

                            JSONObject jsonObject = JSONObject.parseObject(value);
                            JSONObject pageObj = jsonObject.getJSONObject("page");
                            JSONObject commonObj = jsonObject.getJSONObject("common");
                            String pageId = pageObj.getString("page_id");

                            if (
                                    "course_list".equals(pageId)
                                    ||
                                            "course_detail".equals(pageId)
                                    ||
                                            "home".equals(pageId)
                            ) {
                                TrafficPageViewBean trafficPageViewBean =
                                        TrafficPageViewBean
                                                .builder()
                                                .stt("")
                                                .edt("")
                                                .curDate("")
                                                .mid(commonObj.getString("mid"))
                                                .pageId(pageId)
                                                .homeUvCount(0L)
                                                .listUvCount(0L)
                                                .detailUvCount(0L)
                                                .ts(jsonObject.getLong("ts") )
                                                .build();

                                out.collect(trafficPageViewBean);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("过滤脏数据：" + value);
                        }
                    }
                }
        );

        // filterDs.print("🎢🎢");

        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = filterDs
                .keyBy(bean -> bean.getMid())
                .process(
                        new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {

                            private ValueState<String> pageViewDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> pageViewDateDesc = new ValueStateDescriptor<>("pageViewDate", String.class);
                                pageViewDateState = getRuntimeContext().getState(pageViewDateDesc);
                            }

                            @Override
                            public void processElement(TrafficPageViewBean value
                                    , KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>.Context ctx
                                    , Collector<TrafficPageViewBean> out) throws Exception {
                                String lastPageViewDate = pageViewDateState.value();
                                Long ts = value.getTs();
                                String curDate = DateFormatUtil.tsToDate(ts);

                                if (!curDate.equals(lastPageViewDate)) {
                                    String pageId = value.getPageId();
                                    if ("course_list".equals(pageId)) {
                                        value.setListUvCount(1L);
                                        out.collect(value);
                                    } else if ("course_detail".equals(pageId)) {
                                        value.setDetailUvCount(1L);
                                        out.collect(value);
                                    } else if ("home".equals(pageId)) {
                                        value.setHomeUvCount(1L);
                                        out.collect(value);
                                    }
                                    pageViewDateState.update(curDate);
                                }
                            }
                        }
                );

        // beanDs.print("🫛🫛");
        SingleOutputStreamOperator<TrafficPageViewBean> windowsDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                (ele, ts) -> ele.getTs()
                        )
        ).windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setDetailUvCount(value1.getDetailUvCount() + value2.getDetailUvCount());
                        value1.setListUvCount(value1.getListUvCount() + value2.getListUvCount());
                        value1.setHomeUvCount(value1.getHomeUvCount() + value2.getHomeUvCount());
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TrafficPageViewBean, TrafficPageViewBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficPageViewBean, TrafficPageViewBean, TimeWindow>.Context context
                            , Iterable<TrafficPageViewBean> elements
                            , Collector<TrafficPageViewBean> out) throws Exception {

                        TrafficPageViewBean next = elements.iterator().next();
                        next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        next.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        out.collect(next);
                    }
                }
        );

        // windowsDs.print("🪟🪟");
        windowsDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( DORIS_DB_NAME, DWS_TRAFFIC_PAGE_VIEW_WINDOW )
        );

    }
}
