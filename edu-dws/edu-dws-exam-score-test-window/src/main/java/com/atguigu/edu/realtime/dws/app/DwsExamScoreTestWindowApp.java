package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationPaperScoreDurationExamWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
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

public class DwsExamScoreTestWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsExamScoreTestWindowApp()
                .start(10035,3,"dws_exam_score_test_window_app", Constant.TOPIC_DWD_EXAM_TEST_PAPER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.结构转换，补充度量值
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> beanDs = ds.flatMap(
                new FlatMapFunction<String, DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwsExaminationPaperScoreDurationExamWindowBean> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            Double score = jsonObject.getDouble("score");
                            long ts = jsonObject.getLong("ts") * 1000;
                            String scoreDuration;
                            if (score < 60) {
                                scoreDuration = "[0,60)";
                            } else if (score < 70) {
                                scoreDuration = "[60,70)";
                            } else if (score < 80) {
                                scoreDuration = "[70,80)";
                            } else if (score < 90) {
                                scoreDuration = "[80,90)";
                            } else if (score <= 100) {
                                scoreDuration = "[90,100]";
                            } else {
                                scoreDuration = "";
                            }
                            out.collect(DwsExaminationPaperScoreDurationExamWindowBean.builder()
                                    .paper_id(jsonObject.getString("paper_id"))
                                    .score_duration(scoreDuration)
                                    .user_count(1L)
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
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> withWaterDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperScoreDurationExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (element, ts) -> element.getTs()
                        )
        );

        //3.分组、开窗聚合
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> keyByAndWindowDs = withWaterDs.keyBy(
                DwsExaminationPaperScoreDurationExamWindowBean::getPaper_id
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean reduce(DwsExaminationPaperScoreDurationExamWindowBean value1, DwsExaminationPaperScoreDurationExamWindowBean value2) throws Exception {
                        value1.setUser_count(value1.getUser_count() + value2.getUser_count());
                        return value1;
                    }
                }
                , new ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationPaperScoreDurationExamWindowBean> elements, Collector<DwsExaminationPaperScoreDurationExamWindowBean> out) throws Exception {
                        //取出增量聚合的结果
                        DwsExaminationPaperScoreDurationExamWindowBean dwsExaminationPaperScoreDurationExamWindowBean = elements.iterator().next();

                        //补充信息
                        dwsExaminationPaperScoreDurationExamWindowBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        dwsExaminationPaperScoreDurationExamWindowBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        dwsExaminationPaperScoreDurationExamWindowBean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        dwsExaminationPaperScoreDurationExamWindowBean.setTs(System.currentTimeMillis());

                        out.collect(dwsExaminationPaperScoreDurationExamWindowBean);
                    }
                }
        );

        //4.维度关联
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> mapDs = keyByAndWindowDs.map(
                new RichMapFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean>() {
                    Connection connection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean map(DwsExaminationPaperScoreDurationExamWindowBean bean) throws Exception {
                        String tableName = "dim_test_paper";
                        String rowKey = bean.getPaper_id();

                        JSONObject jsonObject = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE, tableName, rowKey);

                        bean.setPaper_title(jsonObject.getString("paper_title"));

                        return bean;
                    }
                }
        );

//        mapDs.print("MD");


        //5.写入到doris
        mapDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink(Constant.DORIS_DB_NAME, Constant.DWS_EXAM_SCORE_TEST_WINDOW)
        );

    }
}
