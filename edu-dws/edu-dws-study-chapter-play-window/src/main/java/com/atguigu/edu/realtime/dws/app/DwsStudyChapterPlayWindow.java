package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsStudyChapterPlayWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Hbck;

import java.time.Duration;

/**
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author: WZY
 * Create Date: 2025/1/9
 * Create Time: 下午8:25
 * Vserion : 1.0
 * TODO
 */
public class DwsStudyChapterPlayWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsStudyChapterPlayWindow().start(10024 , 3 , Constant.DWS_STUDY_CHAPTER_PLAY_WINDOW , Constant.TOPIC_DWD_TRAFFIC_APPVIDEO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<DwsStudyChapterPlayWindowBean> beanStream = ds.map(
                new MapFunction<String, DwsStudyChapterPlayWindowBean>() {
                    @Override
                    public DwsStudyChapterPlayWindowBean map(String value) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject appVideo = jsonObj.getJSONObject("appVideo");
                        DwsStudyChapterPlayWindowBean build = DwsStudyChapterPlayWindowBean.builder()
                                .videoId(appVideo.getString("video_id"))
                                .userId(common.getString("uid"))
                                .playTotalSec(appVideo.getLong("play_sec"))
                                .playCount(1L)
                                .ts(jsonObj.getLong("ts"))
                                .build();
                        return build;
                    }
                }
        );
        // beanStream.print();
        // 分组
        KeyedStream<DwsStudyChapterPlayWindowBean, String> keyedStream = beanStream.keyBy(
                new KeySelector<DwsStudyChapterPlayWindowBean, String>() {
                    @Override
                    public String getKey(DwsStudyChapterPlayWindowBean value) throws Exception {
                        return value.getUserId();
                    }
                }
        );
        // keyedStream.print();
        // 过滤独立用户数
        SingleOutputStreamOperator<DwsStudyChapterPlayWindowBean> uuBeanStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean>() {
                    ValueState<String> lastPlayDtState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastPlayDtStateDesc = new ValueStateDescriptor<>("last_play_dt_state", Types.STRING);
                        lastPlayDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastPlayDtState = getRuntimeContext().getState(lastPlayDtStateDesc);
                    }

                    @Override
                    public void processElement(DwsStudyChapterPlayWindowBean value, KeyedProcessFunction<String, DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean>.Context ctx, Collector<DwsStudyChapterPlayWindowBean> out) throws Exception {
                        String lastDt = lastPlayDtState.value();
                        String curDt = DateFormatUtil.tsToDate(value.getTs());
                        if (lastDt == null || lastDt.compareTo(curDt) < 0) {
                            value.setPlayUuCount(1L);
                            lastPlayDtState.update(curDt);
                        } else {
                            value.setPlayUuCount(0L);
                        }
                        out.collect(value);
                    }
                }
        );
        // uuBeanStream.print();
        SingleOutputStreamOperator<DwsStudyChapterPlayWindowBean> reduceStream = uuBeanStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsStudyChapterPlayWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsStudyChapterPlayWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsStudyChapterPlayWindowBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
                ).keyBy(
                        DwsStudyChapterPlayWindowBean::getVideoId
                ).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<DwsStudyChapterPlayWindowBean>() {
                            @Override
                            public DwsStudyChapterPlayWindowBean reduce(DwsStudyChapterPlayWindowBean value1, DwsStudyChapterPlayWindowBean value2) throws Exception {
                                // 相同组的数据度量值累加
                                value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
                                value1.setPlayTotalSec(value1.getPlayTotalSec() + value2.getPlayTotalSec());
                                value1.setPlayUuCount(value1.getPlayUuCount() + value2.getPlayUuCount());
                                return value1;
                            }
                        }, new ProcessWindowFunction<DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean, String, TimeWindow>.Context context, Iterable<DwsStudyChapterPlayWindowBean> elements, Collector<DwsStudyChapterPlayWindowBean> out) throws Exception {
                                DwsStudyChapterPlayWindowBean next = elements.iterator().next();
                                next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                next.setTs(System.currentTimeMillis());

                                out.collect(next);
                            }
                        }
                );
        // reduceStream.print();
        SingleOutputStreamOperator<DwsStudyChapterPlayWindowBean> chapterStream = reduceStream.map(
                new RichMapFunction<DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean>() {
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
                    public DwsStudyChapterPlayWindowBean map(DwsStudyChapterPlayWindowBean value) throws Exception {
                        String tableName = "dim_video_info";
                        String rowKey = value.getVideoId();
                        JSONObject dimJsonObj = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE_DIM, tableName, rowKey);
                        String chapterId = dimJsonObj.getString("chapter_id");
                        value.setChapterId(chapterId);
                        return value;
                    }
                }
        );
        // chapterStream.print();
        SingleOutputStreamOperator<DwsStudyChapterPlayWindowBean> chapterNameStream = chapterStream.map(
                new RichMapFunction<DwsStudyChapterPlayWindowBean, DwsStudyChapterPlayWindowBean>() {
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
                    public DwsStudyChapterPlayWindowBean map(DwsStudyChapterPlayWindowBean value) throws Exception {
                        String tableName = "dim_chapter_info";
                        String rowKey = value.getChapterId();
                        JSONObject dimJsonObj = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE_DIM, tableName, rowKey);
                        String chapterName = dimJsonObj.getString("chapter_name");
                        value.setChapterName(chapterName);
                        return value;
                    }
                }
        );
        // chapterNameStream.print();
        chapterNameStream.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DORIS_DB_NAME , Constant.DWS_STUDY_CHAPTER_PLAY_WINDOW)
        );
    }
}
