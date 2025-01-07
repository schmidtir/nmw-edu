package com.atguigu.edu.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.base.BaseSqlApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author: WZY
 * Create Date: 2025/1/7
 * Create Time: 下午5:30
 * Vserion : 1.0
 * TODO
 */
public class DwdTrafficUniqueVisitorDetail extends BaseApp {
    public static void main(String[] args) {
       new DwdTrafficUniqueVisitorDetail().start(10012 , 3 , "dwd_traffic_uni_visitor_detail" , Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        KeyedStream<JSONObject, String> keyedStream = ds.map(JSON::parseObject).filter(
                jsonObj -> jsonObj.getJSONObject("page").getString("last_page_id") == null
        ).keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        SingleOutputStreamOperator<JSONObject> filteredDs = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {
                    private ValueState<String> lastVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastVisitDesc = new ValueStateDescriptor<>("last_visit_dt", String.class);
                        lastVisitDesc.enableTimeToLive(
                                StateTtlConfig
                                        .newBuilder(Time.days(1L))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        lastVisitState = getRuntimeContext().getState(lastVisitDesc);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String visitDate = DateFormatUtil.tsToDate(value.getLong("ts"));
                        String lastVisitDate = lastVisitState.value();
                        if (lastVisitDate == null || DateFormatUtil.dateToTs(lastVisitDate) < DateFormatUtil.dateToTs(visitDate)) {
                            lastVisitState.update(visitDate);
                            return true;
                        }
                        return false;
                    }
                }
        );

        SingleOutputStreamOperator<String> resultDs = filteredDs.map(JSON::toString);
        resultDs.print();

        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UNI_VISITOR_DETAIL);


        resultDs.sinkTo(kafkaSink);
    }
}
