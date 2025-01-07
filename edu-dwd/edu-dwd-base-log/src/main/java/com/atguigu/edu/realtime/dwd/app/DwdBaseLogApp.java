package com.atguigu.edu.realtime.dwd.app;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

        // 捕获脏数据的侧输出流
        SideOutputDataStream<String> dirtyDs = etlDs.getSideOutput(dirtyTag);
        // 将脏数据写入 Kafka 对应的主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DIRTY);
        dirtyDs.sinkTo( kafkaSink );
    }
}
