package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsInteractionCourseReviewWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.DimRichMapFunction;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Duration;

public class DwsInteractionCourseReviewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsInteractionCourseReviewWindowApp().start(10033,3,"dws_interaction_course_review_window_app", Constant.TOPIC_DWD_INTERACTION_REVIEW);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.清洗、转换，补充度量值
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> beanDs = ds.flatMap(
                new FlatMapFunction<String, DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwsInteractionCourseReviewWindowBean> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            String courseId = jsonObject.getString("course_id");
                            Long reviewStars = jsonObject.getLong("review_stars");
                            long ts = jsonObject.getLong("ts") * 1000;
                            out.collect(DwsInteractionCourseReviewWindowBean.builder()
                                    .courseId(courseId)
                                    .reviewTotalStars(reviewStars)
                                    .reviewUserCount(1L)
                                    .goodReviewUserCount(reviewStars == 5 ? 1L : 0L)//如果评分等于5星，则将好评用户计数设置为1，否则设置为0。
                                    .ts(ts)
                                    .build());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
        );
//        beanDs.print("BD");

        //2.添加水位线
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> withWaterDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsInteractionCourseReviewWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (element, ts) -> element.getTs()
                        )
        );
//        withWaterDs.print("WB");

        //3.分组、开窗聚合
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> keyByAndWindowDs = withWaterDs.keyBy(
                DwsInteractionCourseReviewWindowBean::getCourseId
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public DwsInteractionCourseReviewWindowBean reduce(DwsInteractionCourseReviewWindowBean value1, DwsInteractionCourseReviewWindowBean value2) throws Exception {
                        value1.setReviewUserCount(value1.getReviewUserCount() + value2.getReviewUserCount());//评分用户数
                        value1.setGoodReviewUserCount(value1.getGoodReviewUserCount() + value2.getGoodReviewUserCount());//好评用户数
                        value1.setReviewTotalStars(value1.getReviewTotalStars() + value2.getReviewTotalStars());//用户总评分
                        return value1;
                    }

                }, new ProcessWindowFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean, String, TimeWindow>.Context context, Iterable<DwsInteractionCourseReviewWindowBean> elements, Collector<DwsInteractionCourseReviewWindowBean> out) throws Exception {
                        //取出增量聚合的结果
                        DwsInteractionCourseReviewWindowBean dwsInteractionCourseReviewWindowBean = elements.iterator().next();
                        //补充信息
                        dwsInteractionCourseReviewWindowBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        dwsInteractionCourseReviewWindowBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        dwsInteractionCourseReviewWindowBean.setCurDate(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        dwsInteractionCourseReviewWindowBean.setTs(System.currentTimeMillis());

                        out.collect(dwsInteractionCourseReviewWindowBean);


                    }
                }
        );

//        keyByAndWindowDs.print("KW");

        //4.维度关联
//        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> courseNameDs = keyByAndWindowDs.map(
//                new RichMapFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean>() {
//                    Connection connection = null;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        connection = HBaseUtil.getConnection();
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        HBaseUtil.closeConnection(connection);
//                    }
//
//                    @Override
//                    public DwsInteractionCourseReviewWindowBean map(DwsInteractionCourseReviewWindowBean bean) throws Exception {
//                        String tableName = "dim_course_info";
//                        String rowKey = bean.getCourseId();
//
//                        JSONObject jsonObject = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE, tableName, rowKey);
//
//                        bean.setCourseName(jsonObject.getString("course_name"));
//
//                        return bean;
//                    }
//                }
//        );

        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> courseNameDs = keyByAndWindowDs.map(
                new DimRichMapFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }

                    @Override
                    public String getRowKey(DwsInteractionCourseReviewWindowBean bean) {
                        return bean.getCourseId();
                    }

                    @Override
                    public void addDim(DwsInteractionCourseReviewWindowBean bean, JSONObject dimJsonObj) {
                        bean.setCourseName(dimJsonObj.getString("course_name"));
                    }
                }
        );

//        courseNameDs.print("CN");

        //5.写出到doris
        courseNameDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink(Constant.DORIS_DB_NAME, Constant.DWS_INTERACTION_COURSE_REVIEW_WINDOW)
        );
    }
}
