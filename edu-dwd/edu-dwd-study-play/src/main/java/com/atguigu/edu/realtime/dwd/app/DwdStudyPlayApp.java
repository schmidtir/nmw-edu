package com.atguigu.edu.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwdLearnPlayBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdStudyPlayApp extends BaseApp {
    public static void main(String[] args) {
        new DwdStudyPlayApp().start(10014,3,"dwd_study_play_app", Constant.TOPIC_DWD_TRAFFIC_APPVIDEO);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        //2.数据清洗与转换
        SingleOutputStreamOperator<DwdLearnPlayBean> learnBeanDs = ds.flatMap(
                new FlatMapFunction<String, DwdLearnPlayBean>() {
                    @Override
                    public void flatMap(String value, Collector<DwdLearnPlayBean> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            JSONObject common = jsonObject.getJSONObject("common");
                            JSONObject appVideo = jsonObject.getJSONObject("appVideo");
                            Long ts = jsonObject.getLong("ts");
                            DwdLearnPlayBean learnPlayBean = DwdLearnPlayBean.builder()
                                    .provinceId(common.getString("ar"))
                                    .brand(common.getString("ba"))
                                    .channel(common.getString("ch"))
                                    .isNew(common.getString("is_new"))
                                    .model(common.getString("md"))
                                    .machineId(common.getString("mid"))
                                    .operatingSystem(common.getString("os"))
                                    .sourceId(common.getString("sc"))
                                    .sessionId(common.getString("sid"))
                                    .userId(common.getString("uid"))
                                    .versionCode(common.getString("vc"))
                                    .playSec(appVideo.getInteger("play_sec"))
                                    .videoId(appVideo.getString("video_id"))
                                    .positionSec(appVideo.getInteger("position_sec"))
                                    .ts(ts)
                                    .build();
                            out.collect(learnPlayBean);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }


                    }
                }
        );
//        learnBeanDs.print();

        //3.添加水位线
        SingleOutputStreamOperator<DwdLearnPlayBean> withWaterDs = learnBeanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwdLearnPlayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (element, ts) -> element.getTs()
                        )
        );
//        withWaterDs.print("WW");

        //4.按会话 ID 分组
        KeyedStream<DwdLearnPlayBean, String> keyByDs = withWaterDs.keyBy(
                new KeySelector<DwdLearnPlayBean, String>() {
                    @Override
                    public String getKey(DwdLearnPlayBean value) throws Exception {
                        return value.getSessionId();
                    }
                }
        );
//        keyByDs.print("KB");

        //5.聚合统计
        WindowedStream<DwdLearnPlayBean, String, TimeWindow> windowDs = keyByDs.window(EventTimeSessionWindows.withGap(Time.seconds(3L)));


        SingleOutputStreamOperator<DwdLearnPlayBean> reduceDs = windowDs.reduce(
                new ReduceFunction<DwdLearnPlayBean>() {
                    @Override
                    public DwdLearnPlayBean reduce(DwdLearnPlayBean value1, DwdLearnPlayBean value2) throws Exception {
                        value1.setPlaySec(value1.getPlaySec() + value2.getPlaySec());
                        if (value2.getTs() > value1.getTs()) {
                            value1.setPositionSec(value2.getPositionSec());
                        }
                        return value1;
                    }
                }, new ProcessWindowFunction<DwdLearnPlayBean, DwdLearnPlayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<DwdLearnPlayBean, DwdLearnPlayBean, String, TimeWindow>.Context context, Iterable<DwdLearnPlayBean> elements, Collector<DwdLearnPlayBean> out) throws Exception {
                        for (DwdLearnPlayBean element : elements) {
                            out.collect(element);
                        }
                    }
                }
        );
        reduceDs.print("WNR");

        //6.结构转换
        SingleOutputStreamOperator<String> jsonMapDs = reduceDs.map(JSON::toJSONString);
        //jsonMapDs.print("JM");

        //7.输出到 Kafka
        jsonMapDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_STUDY_PLAY));

    }
}
