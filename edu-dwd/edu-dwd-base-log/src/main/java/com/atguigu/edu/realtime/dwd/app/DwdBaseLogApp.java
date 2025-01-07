package com.atguigu.edu.realtime.dwd.app;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author: WZY
 * Create Date: 2025/1/7
 * Create Time: 下午2:00
 * Vserion : 1.0
 * TODO
 */
public class DwdBaseLogApp extends BaseApp{
    public static void main(String[] args) {
        new DwdBaseLogApp().start(10011 , 3 , "dwd_base_log_app" , Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // 脏数据 Tag
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag", Types.STRING);
        SingleOutputStreamOperator<JSONObject> etlDs = ds.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            // 转换成 JSONOBJ
                            JSONObject jsonObj = JSON.parseObject(value);
                            // 写出
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            System.out.println("Log分流过滤脏数据：" + value);
                            // 将脏数据写到侧输出流
                            ctx.output(dirtyTag, value);
                        }
                    }
                }
        );

        // etlDs.print("ETL");
/*
        {
        "common":{"sc":"1","ar":"16","uid":"75","os":"Android 11.0","ch":"huawei","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_259","vc":"v2.1.134","ba":"Xiaomi","sid":"bf9f8e30-71b3-49df-82a3-e2e0dd0feaa9"},
        "page":{"page_id":"order","item":"21203","during_time":6071,"item_type":"order_id","last_page_id":"cart"},
        "displays":[
        {"display_type":"promotion","item":"4","item_type":"course_id","pos_id":2,"order":1},
        {"display_type":"query","item":"8","item_type":"course_id","pos_id":4,"order":2},
        {"display_type":"query","item":"2","item_type":"course_id","pos_id":5,"order":3},
        {"display_type":"promotion","item":"3","item_type":"course_id","pos_id":4,"order":4}
        ],
        "ts":1736155411410
        }
*/
        // 捕获脏数据的侧输出流
        SideOutputDataStream<String> dirtyDs = etlDs.getSideOutput(dirtyTag);
        // 将脏数据写入 Kafka 对应的主题中
        // TODO kafkaSink 是不是可以优化一下
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DIRTY);
        dirtyDs.sinkTo( kafkaSink );

        // 维护新老访客标记
        SingleOutputStreamOperator<JSONObject> fixIsNewDs = etlDs.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        ).process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastVisitDateStateDesc = new ValueStateDescriptor<>("lastVisitDateStateDesc", Types.STRING);
                        lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 提取 common
                        JSONObject commonObj = value.getJSONObject("common");
                        // 获取 is_new
                        String isNew = commonObj.getString("is_new");

                        // 获取状态中的值
                        String lastVisitDate = lastVisitDateState.value();

                        // 当前访问日期
                        Long currentTs = value.getLong("ts");
                        String currentVisitDate = DateFormatUtil.tsToDate(currentTs);

                        // 判断
                        // 1. is_new = 1
                        if ("1".equals(isNew)) {
                            if (lastVisitDate == null) {
                                // 状态为 null ， 说明是新访客，将当前数据中的时间作为状态值维护到状态中
                                lastVisitDateState.update(currentVisitDate);
                            } else if (!lastVisitDate.equals(currentVisitDate)) {
                                // 状态不为 null ， 且状态中维护的时间和当前的时间不一致，说明是老访客，将 is_new 修复成 0
                                commonObj.put("is_new", "0");
                            } else {
                                // 状态不为 null ， 状态中维护的数据和当前数据时间一致，说明是新访客且是一天内多次访问，不需要进行修复
                            }
                        } else {
                            // is_new = 0
                            if (lastVisitDate == null) {
                                // 状态为 null ， 说明是老访客 ， 但是数仓中没有记录该访客的状态，将今天之前的一个日期存入状态中
                                lastVisitDateState.update(DateFormatUtil.tsToDate(currentTs - 100 * 60 * 60 * 24));
                            } else {
                                // 状态不为 null ， 说明是老访客，不需要进行修复
                            }
                        }
                        out.collect(value);
                    }
                }
        );
        // 没问题
        // fixIsNewDs.print("FIXED");

        // 写入不同主题
        // 交易域日志页面起始
        OutputTag<String> startTag = new OutputTag<>("startTag", Types.STRING);
        // 交易域日志错误
        OutputTag<String> errTag = new OutputTag<>("errTag", Types.STRING);
        // 交易域日志页面数据
        OutputTag<String> pageTag = new OutputTag<>("pageTag", Types.STRING);
        // 交易域日志行动数据
        OutputTag<String> actionTag = new OutputTag<>("actionTag", Types.STRING);
        // 交易域日志展示数据
        OutputTag<String> displayTag = new OutputTag<>("displayTag", Types.STRING);

        SingleOutputStreamOperator<Object> splitDs = fixIsNewDs.process(
                new ProcessFunction<JSONObject, Object>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, Object>.Context ctx, Collector<Object> out) throws Exception {
                        // 分流错误数据
                        JSONObject errJsonObj = value.getJSONObject("err");
                        if (errJsonObj != null) {
                            // 输出到错误的侧输出流
                            ctx.output(errTag, value.toJSONString());
                            // 从数据中移除 err
                            value.remove("err");
                        }
                        // 分流起始数据
                        JSONObject startJsonObj = value.getJSONObject("start");
                        if (startJsonObj != null) {
                            ctx.output(startTag, value.toJSONString());
                        }

                        JSONObject pageJsonObj = value.getJSONObject("page");
                        JSONObject commonJsonObj = value.getJSONObject("common");
                        Long ts = value.getLong("ts");
                        if (pageJsonObj != null) {
                            // 分流动作数据
                            JSONArray actionJsonArr = value.getJSONArray("actions");
                            if (actionJsonArr != null && actionJsonArr.size() > 0 ) {
                                for (int i = 0; i < actionJsonArr.size(); i++) {
                                    JSONObject actionJsonObj = actionJsonArr.getJSONObject(i);
                                    // 将 action ， page ， common ， ts 处理成一条 json 数据
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("action", actionJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("common", commonJsonObj);

                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                            }
                            // 移除
                            value.remove("actions");
                        }

                        // 分流曝光数据
                        JSONArray displayJsonArr = value.getJSONArray("displays");
                        if (displayJsonArr != null && displayJsonArr.size() > 0) {
                            for (int i = 0; i < displayJsonArr.size(); i++) {
                                JSONObject displayJsonObj = displayJsonArr.getJSONObject(i);
                                // 将 action 、 page 、 common 、ts 处理成一条json数据
                                JSONObject newDisplayJsonObj = new JSONObject();
                                newDisplayJsonObj.put("display", displayJsonObj);
                                newDisplayJsonObj.put("page", pageJsonObj);
                                newDisplayJsonObj.put("common", commonJsonObj);
                                newDisplayJsonObj.put("ts", ts);

                                //输出到曝光的侧输出流中
                                ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                            }

                            // 移除掉
                            value.remove("displays");
                        }

                        // 分流页面数据
                        ctx.output(pageTag, value.toJSONString());
                    }
                }
        );

        // 捕获所有侧输出流
        SideOutputDataStream<String> errDs = splitDs.getSideOutput(errTag);
        SideOutputDataStream<String> startDs = splitDs.getSideOutput(startTag);
        SideOutputDataStream<String> actionDs = splitDs.getSideOutput(actionTag);
        SideOutputDataStream<String> displayDs = splitDs.getSideOutput(displayTag);
        SideOutputDataStream<String> pageDs = splitDs.getSideOutput(pageTag);

        // 没问题
        /*errDs.print("ERR");
        startDs.print("START");
        actionDs.print("ACTION");
        displayDs.print("DISPLAY");
        pageDs.print("PAGE");*/

        // 写入 Kafka 对应主题中
        errDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        actionDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        displayDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

    }
}
