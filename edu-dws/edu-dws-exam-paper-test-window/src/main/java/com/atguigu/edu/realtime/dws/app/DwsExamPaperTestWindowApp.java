package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationPaperExamWindowBean;
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

public class DwsExamPaperTestWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsExamPaperTestWindowApp()
                .start(10034,3,"dws_examination_paper_exam_window_app", Constant.TOPIC_DWD_EXAM_TEST_PAPER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.转换结构，补充度量值
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> beanDs = ds.flatMap(
                new FlatMapFunction<String, DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwsExaminationPaperExamWindowBean> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            long ts = jsonObject.getLong("ts") * 1000;
                            Double score = jsonObject.getDouble("score");

                            out.collect(DwsExaminationPaperExamWindowBean.builder()
                                    .paperId(jsonObject.getString("paper_id"))
                                    .examTakenCount(1L)
                                    .examTotalScore((long) score.doubleValue())
                                    .examTotalDuringSec(jsonObject.getLong("duration_sec"))
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
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withWaterDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (element, ts) -> element.getTs()
                        )
        );
//        withWaterDs.print("WD");


        //3.分组，开窗聚合
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> keyByAndWindowDs = withWaterDs.keyBy(
                DwsExaminationPaperExamWindowBean::getPaperId
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        ).reduce(
                new ReduceFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperExamWindowBean reduce(DwsExaminationPaperExamWindowBean value1, DwsExaminationPaperExamWindowBean value2) throws Exception {
                        value1.setExamTakenCount(value1.getExamTakenCount() + value2.getExamTakenCount());
                        value1.setExamTotalScore(value1.getExamTotalScore() + value2.getExamTotalScore());
                        value1.setExamTotalDuringSec(value1.getExamTotalDuringSec() + value2.getExamTotalDuringSec());
                        return value1;
                    }
                }
                , new ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationPaperExamWindowBean> elements, Collector<DwsExaminationPaperExamWindowBean> out) throws Exception {
                        //取出增量聚合的结果
                        DwsExaminationPaperExamWindowBean dwsExaminationPaperExamWindowBean = elements.iterator().next();

                        //补充信息
                        dwsExaminationPaperExamWindowBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        dwsExaminationPaperExamWindowBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        dwsExaminationPaperExamWindowBean.setTs(System.currentTimeMillis());

                        out.collect(dwsExaminationPaperExamWindowBean);
                    }
                }
        );

        //4.维度关联
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> mapDs = keyByAndWindowDs.map(
                new RichMapFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean>() {
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
                    public DwsExaminationPaperExamWindowBean map(DwsExaminationPaperExamWindowBean bean) throws Exception {
                        String tableName = "dim_test_paper";
                        String rowKey = bean.getPaperId();

                        JSONObject jsonObject = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE, tableName, rowKey);

                        bean.setPaperTitle(jsonObject.getString("paper_title"));
                        bean.setCourseId(jsonObject.getString("course_id"));

                        return bean;
                    }
                }
        );

        mapDs.print("MD");

        //5.写出到doris
        mapDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink(Constant.DORIS_DB_NAME,Constant.DWS_EXAM_PAPER_TEST_WINDOW)
        );
    }
}
