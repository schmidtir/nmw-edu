package com.atguigu.edu.realtime.dim;

/* *
 * Package Name: com.atguigu.edu.realtime.dim
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：20:49
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.JdbcUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp()
                .start(10001,3,"DIM_APP",TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print("INPUT");
        //  {
        //  "database":"edu"
        //  ,"table":"user_chapter_process"
        //  ,"type":"insert"
        //  ,"ts":1736155411
        //  ,"xid":180968
        //  ,"commit":true
        //  ,"data":{"id":2948,"course_id":194,"chapter_id":24385,"user_id":952,"position_sec":259,"create_time":"2025-01-06 17:23:31","update_time":null,"deleted":"0"}
        //  }

        SingleOutputStreamOperator<JSONObject> etlDs = ds.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value
                            , ProcessFunction<String, JSONObject>.Context ctx
                            , Collector<JSONObject> out) throws Exception {
                        try {
                            //转换数据为 JSONObj
                            JSONObject jsonObject = JSON.parseObject(value);

                            // 提取 database
                            String database = jsonObject.getString("database");

                            // 提取 type
                            String type = jsonObject.getString("type");

                            // 提取 data
                            JSONObject dataJsonObj
                                    = jsonObject.getJSONObject("data");

                            //判断处理
                            if (
                                    DB_NAME.equals(database)
                                            &&
                                            (MAXWELL_TYPE_INSERT.equals(type) || MAXWELL_TYPE_UPDATE.equals(type) || MAXWELL_TYPE_DELETE.equals(type) || MAXWELL_TYPE_BOOTSTRAP_INSERT.equals(type))
                                            &&
                                            dataJsonObj != null && dataJsonObj.size() >= 2
                            ) {
                                out.collect(jsonObject);
                            }

                        } catch (Exception e) {
                            throw new RuntimeException(" DimApp => 过滤掉脏数据" + value);
                        }

                    }
                }
        );

        // etlDs.print("🎢🎢");

        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(CONFIG_DB_NAME, TABLE_PROCESS_DIM);
        DataStreamSource<String> configDs =
                env.fromSource( mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource" )
                        .setParallelism( 1 );

        // configDs.print();

        SingleOutputStreamOperator<TableProcessDim> tpdDs = configDs.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        // 将整个字符串转换成Json 对象类型
                        JSONObject jsonObj = JSON.parseObject(value);

                        String op = jsonObj.getString("op");

                        TableProcessDim tableProcessDim;
                        if (CDC_OP_D.equals(op)) {
                            // d，取 before
//                            String beforeJsonStr = jsonObj.getString("before");
//                            JSON.parseObject(beforeJsonStr,TableProcessDim.class);

                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            // c,r,u， 取 after
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }

                        // 补充 op
                        tableProcessDim.setOp(op);

                        try {
                            return tableProcessDim;
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            throw new RuntimeException(" DimApp => " + value);
                        }
                    }
                }
        ).setParallelism( 1 );

        // tpdDs.print("🧊🧊");

        SingleOutputStreamOperator<TableProcessDim> createOrDelDs = tpdDs.process(
                new ProcessFunction<TableProcessDim, TableProcessDim>() {

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
                    public void processElement(TableProcessDim value
                            , ProcessFunction<TableProcessDim, TableProcessDim>.Context ctx
                            , Collector<TableProcessDim> out) throws Exception {

                        String op = value.getOp();

                        if (CDC_OP_D.equals(op)) {
                            // 删除表
                            dropHBaseTable(value);
                        } else if (CDC_OP_U.equals(op)) {
                            // 先删表再建表
                            dropHBaseTable(value);
                            createHBaseTable(value);
                        } else {
                            // 建表
                            createHBaseTable(value);
                        }

                        out.collect(value);

                    }

                    private void dropHBaseTable(TableProcessDim tableProcessDim) {
                        HBaseUtil.dropTable(connection, HBASE_NAMESPACE_DIM, tableProcessDim.getSinkTable());
                    }

                    private void createHBaseTable(TableProcessDim tableProcessDim) {
                        HBaseUtil.createTable(connection, HBASE_NAMESPACE_DIM, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
                    }
                }
        ).setParallelism( 1 );

        // createOrDelDs.print("😯😯");

        MapStateDescriptor<String, TableProcessDim> dimMapState = new MapStateDescriptor<>("dimMapState", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> broadcast = createOrDelDs.broadcast( dimMapState );

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterDs = etlDs.connect(broadcast)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                            private Map<String, TableProcessDim> configMap = new HashMap<String, TableProcessDim>();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 查询语句
                                String sql =
                                        " SELECT source_table" +
                                                " , sink_table " +
                                                " , sink_family " +
                                                " , sink_columns " +
                                                " , sink_row_key " +
                                                " from " + CONFIG_DB_NAME + "." + TABLE_PROCESS_DIM;
                                // 获取连接
                                java.sql.Connection connection = JdbcUtil.getConnection();

                                // 执行查询
                                List<TableProcessDim> configList = JdbcUtil.queryList(connection, sql, TableProcessDim.class, true);

//                                for (TableProcessDim tableProcessDim : configList) {
//                                    System.out.println(tableProcessDim);
//                                }
                                // 关闭连接
                                JdbcUtil.closeConnection(connection);
                                // configList 转化为 Map
                                configList.forEach(tableProcessDim -> configMap.put(tableProcessDim.getSourceTable(), tableProcessDim));

                            }

                            @Override
                            public void processElement(JSONObject jsonObj
                                    , BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx
                                    , Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
// 提取广播状态
                                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(dimMapState);

                                // 提取 table
                                String table = jsonObj.getString("table");

                                // 判断广播状态中是否包含 table
//                                TableProcessDim tableProcessDim = broadcastState.get(table);
                                TableProcessDim tableProcessDim = null;

                                if (
                                        (tableProcessDim = broadcastState.get(table)) != null // 优先从状态中取
                                                ||
                                                (tableProcessDim = configMap.get(table)) != null // 再从预加载的 Map 中取
                                ) {
                                    // 状态中有，说明就是维度表数据

                                    // 将维度表数据中的 data 写到下游，同时将 type 补充到 data 中
                                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                                    // 过滤掉不需要的字段
                                    List<String> needColumns = Arrays.asList(tableProcessDim.getSinkColumns().split(","));
                                    // 移除不需要的字段
                                    dataJsonObj.keySet().removeIf(key -> !needColumns.contains(key));


                                    dataJsonObj.put("type", jsonObj.getString("type"));
                                    // 写出数据

                                    if( dataJsonObj == null || tableProcessDim == null) {
                                        System.out.println(dataJsonObj);
                                        System.out.println(tableProcessDim);
                                    }
                                    out.collect(Tuple2.of(dataJsonObj, tableProcessDim));

                                }

                            }

                            @Override
                            public void processBroadcastElement(TableProcessDim value
                                    , BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx
                                    , Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

                                // 提取广播状态
                                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(dimMapState);

                                // 操作类型
                                String op = value.getOp();

                                // 按照操作类型决定状态执行哪些操作
                                if (CDC_OP_D.equals(op)) {
                                    // 从状态中删除维度表
                                    broadcastState.remove(value.getSourceTable());
                                    // 从预加载 Map 中删除维度表
                                    configMap.remove(value.getSourceTable());
                                } else {
                                    // c,r,u
                                    broadcastState.put(value.getSourceTable(), value);
                                    // 往预加载中添加数据（可选）
                                    configMap.put(value.getSourceTable(), value);
                                }

                            }
                        }
                );

        // filterDs.print("🫛🫛");

        filterDs.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {

                    Connection connection = null;
//                    Jedis jedis = null;
                    /**
                     * 创建连接
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取Hbase 连接对象
                        connection = HBaseUtil.getConnection();
//                        jedis = RedisUtil.getJedis();
                    }

                    /***
                     * 关闭连接
                     */
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeConnection( connection );
//                        RedisUtil.closeJedis( jedis );
                    }

                    /***
                     * 数据写出
                     * @param value The input record.
                     * @param context Additional context about the input record.
                     * @throws Exception
                     */
                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> value
                            , Context context) throws Exception {
                        // 维度数据
                        JSONObject dataJsonObj = value.f0;

                        // 配置表数据
                        TableProcessDim tableProcessDim = value.f1;

                        // 从 dataJsonObj 中取出 type， 并移除， 不需要写到维度表中
//                        String type = dataJsonObj.getString("type");
                        // 取出并且移除
                        String type = dataJsonObj.remove("type").toString();

                        // 获取 rowKey列
                        String rowKeyCol= tableProcessDim.getSinkRowKey();

                        // 获取 rowKey 值
                        String rowKey = dataJsonObj.getString( rowKeyCol );

                        // 获取列族
                        String sinkFamily = tableProcessDim.getSinkFamily();

                        if( MAXWELL_TYPE_DELETE.equals(type) ) {
                            // 执行删除
                            HBaseUtil.delRow( connection, HBASE_NAMESPACE_DIM, tableProcessDim.getSinkTable() , rowKey );
                        }else{
                            // 执行写入
                            HBaseUtil.putRow( connection, HBASE_NAMESPACE_DIM, tableProcessDim.getSinkTable() , rowKey, sinkFamily ,dataJsonObj);
                        }

//                        if ( MAXWELL_TYPE_DELETE.equals(type) || MAXWELL_TYPE_UPDATE.equals(type) ) {
//                            // 删除 Redis 中对应的维度数据
//                            RedisUtil.deleteDim( jedis, tableProcessDim.getSinkTable(), rowKey );
//                        }
                    }
                }
        );

    }
}
