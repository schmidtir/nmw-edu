package com.atguigu.edu.realtime.dwd.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author : Kevin
 * Create Date ：2025/1/7
 * Create Time ：14:10
 * TODO 业务表动态分流
 * <p>
        交易域加购事务事实表	dwd_trade_cart_add
        考试域答卷事务事实表	dwd_exam_test_paper
        考试域答题事务事实表	dwd_exam_test_exam_question
     互动域收藏课程事务事实表	dwd_interaction_favor_add
     互动域章节评价事务事实表	dwd_interaction_comment_info
     互动域课程评价事务事实表	dwd_interaction_review
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwdBaseDbApp extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseDbApp()
                .start(10019,3,"dwd_base_db_app",TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print("INPUT");
        // {
        //     "database": "edu",
        //         "table": "user_chapter_process",
        //         "type": "insert",
        //         "ts": 1736155411,
        //         "xid": 180968,
        //         "commit": true,
        //         "data": {}
        // }
        // 数据清洗
        SingleOutputStreamOperator<JSONObject> etlDs = ds.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value
                            , ProcessFunction<String, JSONObject>.Context ctx
                            , Collector<JSONObject> out) {

                        try {
                            // calue 转换为 JSON
                            JSONObject jsonObject = JSON.parseObject(value);
                            String database = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            JSONObject dataJsonObj = jsonObject.getJSONObject("data");

                            if (
                                    DB_NAME.equals(database)
                                            &&
                                            MAXWELL_TYPE_INSERT.equals(type)
                                            &&
                                            dataJsonObj != null && dataJsonObj.size() >= 2
                            ) {
                                out.collect(jsonObject);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(" DWD_BASE_DB ==> 过滤脏数据 : " + value);
                        }
                    }
                }
        );

        // etlDs.print("🎢🎢");
        // {
        //     "database": "edu",
        //         "xid": 180762,
        //         "data": {...},
        //     "commit": true,
        //         "type": "insert",
        //         "table": "review_info",
        //         "ts": 1736155411
        // }

        // 使用 FlinkCDC读取配置表的数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(CONFIG_DB_NAME, TABLE_PROCESS_DWD);
        DataStreamSource<String> configDs =
                env.fromSource( mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource" )
                        .setParallelism( 1 );
        // configDs.print("🙌🙌");
        // {
        //     "before": null,
        //         "after": {},
        //     "source": {},
        //     "op": "r",
        //         "ts_ms": 1736231879076,
        //         "transaction": null
        // }

        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdDs = configDs.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String value) throws Exception {
                        // 将整个字符串转换成Json    对象类型
                        JSONObject jsonObject = JSON.parseObject(value);

                        // 获取 op(判断后续是否要增删 主题 )
                        String op = jsonObject.getString("op");

                        // 保留要传出的结果值
                        TableProcessDwd tableProcessDwd = null;

                        if (CDC_OP_D.equals(op)) {
                            // 取 before
                            tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                        } else {
                            // 取 after
                            tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                        }

                        // 在要输出的对象中补充 op
                        tableProcessDwd.setOp(op);

                        try {
                            return tableProcessDwd;
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(" 获取 dwd_meta 信息失败 !!! ");
                        }
                    }
                }
        ).setParallelism( 1 );

        // tableProcessDwdDs.print("🧊🧊");
        // TableProcessDwd(
        //      sourceTable=test_exam_question
        //      , sourceType=insert
        //      , sinkTable=dwd_exam_test_exam_question
        //      , sinkColumns=id,exam_id,paper_id,question_id,user_id,is_correct,score,create_time,update_time
        //      , op=r)

        // 配置流转换为广播流
        MapStateDescriptor<String, TableProcessDwd> dwdMapState =
                new MapStateDescriptor<>( "dwdMapState", String.class, TableProcessDwd.class );

        BroadcastStream<TableProcessDwd> broadcastDs = tableProcessDwdDs.broadcast(dwdMapState);

        // 合并数据流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connectDs = etlDs.connect(broadcastDs)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {

                            private Map<String, TableProcessDwd> configMap = new HashMap<String, TableProcessDwd>();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                String sql =
                                        " select " +
                                                " source_table , source_type , sink_table , sink_columns  " +
                                                " from " + CONFIG_DB_NAME + "." + TABLE_PROCESS_DWD;

                                // 获取连接
                                Connection connection = JdbcUtil.getConnection();
                                // 执行查询
                                List<TableProcessDwd> configList =
                                        JdbcUtil.queryList(
                                                connection
                                                , sql
                                                , TableProcessDwd.class
                                                , true
                                        );

                                // 关闭连接
                                JdbcUtil.closeConnection(connection);

                                // 将List 转换成Map结构
                                configList.forEach(tableProcessDwd
                                        -> configMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd));

                            }

                            @Override
                            public void processElement(JSONObject value
                                    , BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx
                                    , Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                                // 提取广播状态
                                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(dwdMapState);

                                // 提取数据流中的 table
                                String table = value.getString("table");
                                String type = value.getString("type");
                                String key = table + ":" + type;

                                // 判断数据流中含有 广播状态 含有的 table 数据
                                TableProcessDwd tableProcessDwd = null;
                                if (
                                        (tableProcessDwd = broadcastState.get(key)) != null
                                                ||
                                                (tableProcessDwd = configMap.get(key)) != null
                                ) {
                                    // 能在状态中找到 table，则将这条 value 输出
                                    JSONObject dataJsonObj = value.getJSONObject("data");

                                    // 过滤不需要的字段
                                    List<String> needCols = Arrays.asList(tableProcessDwd.getSinkColumns().split(","));
                                    dataJsonObj.keySet().removeIf(k -> !needCols.contains(k));

                                    // 向下游传递数据前，补充 ts 事件时间到 JSONObject value 中
                                    Long ts = value.getLong("ts");
                                    dataJsonObj.put("ts", ts);

                                    // 写出数据
                                    out.collect(Tuple2.of(dataJsonObj, tableProcessDwd));
                                }
                            }

                            @Override
                            public void processBroadcastElement(TableProcessDwd value
                                    , BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx
                                    , Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                                // 提取广播状态
                                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(dwdMapState);

                                // 获取操作类型
                                String op = value.getOp();

                                String key = value.getSourceTable() + ":" + value.getSourceType();
                                // 根据 op 判断所需要执行的操作
                                if (CDC_OP_D.equals(op)) {
                                    // 状态中删除dwd
                                    broadcastState.remove(key);
                                    configMap.remove(key);
                                } else {
                                    // 往状态中添加 dwd
                                    broadcastState.put(key, value);
                                    configMap.put(key, value);
                                }
                            }
                        }
                );

        // connectDs.print("🪟🪟");
        // ({"course_id":61
        //      ,"create_time":"2025-01-06 17:23:32"
        //      ,"user_id":20
        //      ,"id":2861
        //      ,"review_stars":5
        //      ,"ts":1736155411}
        //,TableProcessDwd(
        //          sourceTable=review_info
        //          , sourceType=insert
        //          , sinkTable=dwd_interaction_review
        //          , sinkColumns=id,user_id,course_id,review_stars,create_time
        //          , op=null))

        // 动态分流
        connectDs.sinkTo(FlinkSinkUtil.getDwdKafkaSink());
    }
}
