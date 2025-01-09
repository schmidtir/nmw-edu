package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationQuestionAnswerWindowBean;
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

public class DwsExamQuestionsTestWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsExamQuestionsTestWindowApp().start(10036,3,"dws_exam_questions_test_window_app", Constant.TOPIC_DWD_EXAM_TEST_EXAM_QUESTION);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.结构转换，补充度量值
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> beanDs = ds.flatMap(
                new FlatMapFunction<String, DwsExaminationQuestionAnswerWindowBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwsExaminationQuestionAnswerWindowBean> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String isCorrect = jsonObject.getString("is_correct");
                        long ts = jsonObject.getLong("ts") * 1000;
                        out.collect(DwsExaminationQuestionAnswerWindowBean.builder()
                                .question_id(jsonObject.getString("question_id"))
                                .answer_count(1L)
                                .correctAnswerCount("1".equals(isCorrect) ? 1L : 0L)
                                .ts(ts)
                                .build());
                    }
                }
        );
//        beanDs.print("BD");

        //2.添加水位线
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> withWaterDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationQuestionAnswerWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (element, ts) -> element.getTs()
                        )
        );
//        withWaterDs.print("WD");

        //3.分组、开窗聚合
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> keyByAndWindowDs = withWaterDs.keyBy(DwsExaminationQuestionAnswerWindowBean::getQuestion_id)
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(10L))
                ).reduce(
                        new ReduceFunction<DwsExaminationQuestionAnswerWindowBean>() {
                            @Override
                            public DwsExaminationQuestionAnswerWindowBean reduce(DwsExaminationQuestionAnswerWindowBean value1, DwsExaminationQuestionAnswerWindowBean value2) throws Exception {

                                value1.setAnswer_count(value1.getAnswer_count() + value2.getAnswer_count());
                                value1.setCorrectAnswerCount(value1.getCorrectAnswerCount() + value2.getCorrectAnswerCount());
                                return value1;
                            }
                        }
                        , new ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationQuestionAnswerWindowBean> elements, Collector<DwsExaminationQuestionAnswerWindowBean> out) throws Exception {
                                //取出增量聚合的结果
                                DwsExaminationQuestionAnswerWindowBean dwsExaminationQuestionAnswerWindowBean = elements.iterator().next();

                                //补充信息
                                dwsExaminationQuestionAnswerWindowBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                dwsExaminationQuestionAnswerWindowBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                dwsExaminationQuestionAnswerWindowBean.setCurDate(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                dwsExaminationQuestionAnswerWindowBean.setTs(System.currentTimeMillis());


                                out.collect(dwsExaminationQuestionAnswerWindowBean);
                            }
                        }
                );

//        keyByAndWindowDs.print("KW");

        //4.维度关联
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> mapDs = keyByAndWindowDs.map(
                new RichMapFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean>() {
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
                    public DwsExaminationQuestionAnswerWindowBean map(DwsExaminationQuestionAnswerWindowBean bean) throws Exception {
                        String tableName = "dim_test_question_info";
                        String rowKey = bean.getQuestion_id();

                        JSONObject jsonObject = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE, tableName, rowKey);

                        bean.setQuestion_txt(jsonObject.getString("question_txt"));

                        return bean;
                    }
                }
        );

        mapDs.print("MD");

        //5.输出到doris
        mapDs.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink(Constant.DORIS_DB_NAME, Constant.DWS_EXAM_QUESTIONS_TEST_WINDOW)
        );
    }
}
