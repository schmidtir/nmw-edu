package edu.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Package Name: edu.realtime.dwd.app
 * Author: WZY
 * Create Date: 2025/1/7
 * Create Time: 下午8:10
 * Vserion : 1.0
 * TODO
 */
public class DwdTrafficUserJumpDetail extends BaseApp {
    public static void main(String[] args) {
        new DwdTrafficUserJumpDetail().start(10013 , 3 , "dwd_traffic_user_jump" , Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<JSONObject> jsonDs = ds.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> withWatermarks = jsonDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }
                )
        );
        KeyedStream<JSONObject, String> keyedStream = withWatermarks.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                }
        );
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).next("second").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).within(Time.seconds(10L));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        OutputTag<String> timeOutTag = new OutputTag<String>("timeOutTag" , Types.STRING);
        SingleOutputStreamOperator<String> flatSelectDs = patternStream.flatSelect(timeOutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                JSONObject first = map.get("first").get(0);
                collector.collect(first.toJSONString());
            }
        }, new PatternFlatSelectFunction<JSONObject, String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                JSONObject first = map.get("first").get(0);
                collector.collect(first.toJSONString());
            }
        });
        SideOutputDataStream<String> timeOutStream = flatSelectDs.getSideOutput(timeOutTag);
        DataStream<String> unionStream = flatSelectDs.union(timeOutStream);
        unionStream.print();
        unionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_USER_JUMP));
    }
}
