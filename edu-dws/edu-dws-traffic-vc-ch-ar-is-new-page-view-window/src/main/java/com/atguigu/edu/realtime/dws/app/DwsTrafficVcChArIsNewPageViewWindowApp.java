package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTrafficForSourcePvBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.DorisMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Hbck;

import javax.swing.*;
import java.time.Duration;

/**
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author: WZY
 * Create Date: 2025/1/8
 * Create Time: 下午7:57
 * Vserion : 1.0
 * TODO
 */
public class DwsTrafficVcChArIsNewPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindowApp().start(10022 , 3 , Constant.DWS_TRAFFIC_VC_SOURCE_AR_IS_NEW_PAGE_VIEW_WINDOW , Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // 清洗 转换
        SingleOutputStreamOperator<JSONObject> jsonDs = ds.map(JSON::parseObject);
        KafkaSource<String> pageSource = FlinkSourceUtil.getKafkaSource( Constant.DWS_TRAFFIC_VC_SOURCE_AR_IS_NEW_PAGE_VIEW_WINDOW , Constant.TOPIC_DWD_TRAFFIC_PAGE);
        DataStreamSource<String> pageStream = env.fromSource(pageSource, WatermarkStrategy.noWatermarks(), "page_log");

        KafkaSource<String> uvSource = FlinkSourceUtil.getKafkaSource(Constant.DWS_TRAFFIC_VC_SOURCE_AR_IS_NEW_PAGE_VIEW_WINDOW , Constant.TOPIC_DWD_TRAFFIC_PAGE);
        DataStreamSource<String> uvStream = env.fromSource(uvSource, WatermarkStrategy.noWatermarks(), "uv_detail");

        KafkaSource<String> jumpSource = FlinkSourceUtil.getKafkaSource(Constant.DWS_TRAFFIC_VC_SOURCE_AR_IS_NEW_PAGE_VIEW_WINDOW , Constant.TOPIC_DWD_TRAFFIC_PAGE);
        DataStreamSource<String> jumpStream = env.fromSource(jumpSource, WatermarkStrategy.noWatermarks(), "jump_detail");

        // pageStream.print("PAGE");
        // uvStream.print();
        // jumpStream.print();

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> pageBeanStream = pageStream.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String value) throws Exception {
                        // 将 page_log 的一条日志转换为一个对应的 javabean
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");
                        DwsTrafficForSourcePvBean build = DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .provinceId(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(0L)
                                .totalSessionCount(page.getString("last_page_id") == null ? 1L : 0L)
                                .pageViewCount(1L)
                                .totalDuringTime(page.getLong("during_time"))
                                .jumpSessionCount(0L)
                                .ts(ts)
                                .build();
                        return build;
                    }
                }
        );
        // pageStream.print();
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> uvBeanStream = uvStream.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String value) throws Exception {
                        // 将page_log的一条日志转换为一个对应的 javabean
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject common = jsonObj.getJSONObject("common");
                        Long ts = jsonObj.getLong("ts");
                        DwsTrafficForSourcePvBean build = DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .provinceId(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(1L)
                                .totalSessionCount(0L)
                                .pageViewCount(0L)
                                .totalDuringTime(0L)
                                .jumpSessionCount(0L)
                                .ts(ts)
                                .build();
                        return build;
                    }
                }
        );
        // uvBeanStream.print();
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> jumpBeanStream = jumpStream.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String value) throws Exception {
                        // 将 日志转换为对应的 bean
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject common = jsonObj.getJSONObject("common");
                        Long ts = jsonObj.getLong("ts");
                        DwsTrafficForSourcePvBean build = DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .provinceId(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(0L)
                                .totalSessionCount(0L)
                                .pageViewCount(0L)
                                .totalDuringTime(0L)
                                .jumpSessionCount(1L)
                                .ts(ts)
                                .build();
                        return build;
                    }
                }
        );
        // jumpBeanStream.print();
        DataStream<DwsTrafficForSourcePvBean> unionStream = pageBeanStream.union(uvBeanStream).union(jumpBeanStream);
        // unionStream.print();

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withWaterMarkStream = unionStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTrafficForSourcePvBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTrafficForSourcePvBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTrafficForSourcePvBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // withWaterMarkStream.print();

        WindowedStream<DwsTrafficForSourcePvBean, String, TimeWindow> windowedStream = withWaterMarkStream.keyBy(
                new KeySelector<DwsTrafficForSourcePvBean, String>() {
                    @Override
                    public String getKey(DwsTrafficForSourcePvBean value) throws Exception {
                        return value.getVersionCode()
                                + value.getSourceId()
                                + value.getSourceId()
                                + value.getProvinceId()
                                + value.getIsNew();
                    }
                }
        ).window(TumblingEventTimeWindows.of(Time.seconds(10L)));


        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> reduceStream = windowedStream.reduce(
                new ReduceFunction<DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean reduce(DwsTrafficForSourcePvBean value1, DwsTrafficForSourcePvBean value2) throws Exception {
                        value1.setTotalSessionCount(value1.getTotalSessionCount() + value2.getTotalSessionCount());
                        value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                        value1.setTotalDuringTime(value1.getTotalDuringTime() + value2.getTotalDuringTime());
                        value1.setJumpSessionCount(value1.getJumpSessionCount() + value2.getJumpSessionCount());
                        value1.setPageViewCount(value1.getPageViewCount() + value2.getPageViewCount());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, String, TimeWindow>.Context context, Iterable<DwsTrafficForSourcePvBean> elements, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                        DwsTrafficForSourcePvBean trafficForSourcePvBean = elements.iterator().next();
                        trafficForSourcePvBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        trafficForSourcePvBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        trafficForSourcePvBean.setTs(System.currentTimeMillis());

                        out.collect(trafficForSourcePvBean);
                    }
                }
        );
        // reduceStream.print();
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> sourceSiteStream = reduceStream.map(
                new RichMapFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean>() {
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
                    public DwsTrafficForSourcePvBean map(DwsTrafficForSourcePvBean value) throws Exception {
                        String tableName = "dim_base_source";
                        String rowKey = value.getSourceId();
                        JSONObject dimJsonObj = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE_DIM, tableName, rowKey);
                        String sourceName = dimJsonObj.getString("source_site");
                        value.setSourceName(sourceName);
                        return value;
                    }
                }
        );
        // sourceSiteStream.print();
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> provinceNameStream = sourceSiteStream.map(
                new RichMapFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean>() {
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
                    public DwsTrafficForSourcePvBean map(DwsTrafficForSourcePvBean value) throws Exception {
                        String tableName = "dim_base_province";
                        String rowKey = value.getProvinceId();
                        JSONObject dimJsonObj = HBaseUtil.getRow(connection, Constant.HBASE_NAMESPACE_DIM, tableName, rowKey);
                        String provinceName = dimJsonObj.getString("name");
                        value.setProvinceName(provinceName);
                        return value;
                    }
                }
        );
        // provinceNameStream.print();
        provinceNameStream.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DORIS_DB_NAME , Constant.DWS_TRAFFIC_VC_SOURCE_AR_IS_NEW_PAGE_VIEW_WINDOW)
        );
    }
}
