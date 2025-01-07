package com.atguigu.edu.realtime.dwd.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author : Kevin
 * Create Date ：2025/1/7
 * Create Time ：15:48
 * TODO 交易域下单事务事实表
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseSqlApp;
import com.atguigu.edu.realtime.common.util.FlinkSqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwdTradeOrderDetailApp extends BaseSqlApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderDetailApp()
                .start( 10017,3, "dwd_trade_order_detail_app_t");
    }
    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception {

        // 设置TTL
        streamTableEnv.getConfig().setIdleStateRetention( Duration.ofSeconds( 15 ) );

        // 过滤业务数据
        readOdsTopicDb( streamTableEnv, "dwd_trade_order_detail_app_db" );

        // streamTableEnv.executeSql(" SELECT * FROM topic_db ").print();

        // 读取 dwd_traffic_page_log
        streamTableEnv.executeSql(
                " CREATE TABLE page_log ( \n" +
                        " `common` map<STRING,STRING> \n" +
                        " , `page` map<STRING,STRING> \n" +
                        " , `ts` STRING \n" +
                        " ) " + FlinkSqlUtil.getKafkaSourceDDL( "dwd_traffic_page", "dwd_trade_order_detail_app_log" )
        );

        // 过滤订单表详情
        // table = "order_detail" and type = "insert"
        Table orderDetailTable = streamTableEnv.sqlQuery(
                " SELECT \n" +
                        "  `data`['id'] AS  id  \n" +
                        " , `data`['course_id'] AS  course_id  \n" +
                        " , `data`['course_name'] AS  course_name  \n" +
                        " , `data`['order_id'] AS  order_id  \n" +
                        " , `data`['user_id'] AS  user_id  \n" +
                        " , `data`['origin_amount'] AS  origin_amount  \n" +
                        " , `data`['coupon_reduce'] AS  coupon_reduce  \n" +
                        " , `data`['final_amount'] AS  final_amount  \n" +
                        " , `data`['create_time'] AS  create_time  \n" +
                        " , date_format( `data`['create_time'], 'yyyy-MM--dd' ) AS create_date \n" +
                        " , ts \n" +
                        " from topic_db \n" +
                        " WHERE `database` = '" + DB_NAME + "'  \n" +
                        " AND `table` = 'order_detail' \n" +
                        " AND `type` = '" + MAXWELL_TYPE_INSERT + "' "
        );

        // orderDetailTable.execute().print();
        streamTableEnv.createTemporaryView("order_detail", orderDetailTable);

        // 过滤订单表

        Table orderInfoTable = streamTableEnv.sqlQuery(
                " SELECT \n " +
                        "  `data`['id'] AS id \n" +
                        " , `data`['user_id'] AS user_id \n" +
                        " , `data`['out_trade_no'] AS out_trade_no \n" +
                        " , `data`['trade_body'] AS trade_body  \n" +
                        " , `data`['session_id'] AS session_id  \n" +
                        " , `data`['province_id'] AS province_id  \n" +
                        " , date_format( `data`['create_time'], 'yyyy-MM--dd' ) AS create_date \n" +
                        " , ts \n" +
                        " from topic_db \n" +
                        " WHERE `database` = '" + DB_NAME + "'  \n" +
                        " AND `table` = 'order_info' \n" +
                        " AND `type` = '" + MAXWELL_TYPE_INSERT + "' "
        );

        // orderInfoTable.execute().print();
        streamTableEnv.createTemporaryView("order_info", orderInfoTable);

        // 获取下单日志
        Table orderLogTable = streamTableEnv.sqlQuery(
                " SELECT \n" +
                        " `common`['sid'] AS session_id \n" +
                        " , `common`['sc'] AS source_id \n" +
                        " FROM page_log \n" +
                        " WHERE `page`['page_id'] = 'order' "
        );

        // orderLogTable.execute().print();
        streamTableEnv.createTemporaryView("order_log", orderLogTable);

        // 关联3张表
        Table resultTable = streamTableEnv.sqlQuery(
                " SELECT \n" +
                        " od.id AS id\n" +
                        " , od.course_id AS course_id \n" +
                        " , od.course_name AS course_name \n" +
                        " , od.order_id AS order_id \n" +
                        " , od.user_id AS user_id \n" +
                        " , od.origin_amount AS origin_amount \n" +
                        " , od.coupon_reduce AS coupon_reduce \n" +
                        " , od.final_amount AS final_amount \n" +
                        " , od.create_time AS create_time \n" +
                        " , od.create_date AS create_date \n" +
                        " , od.ts AS ts \n" +
                        " , oi.user_id AS user_id \n" +
                        " , oi.out_trade_no AS out_trade_no \n" +
                        " , oi.trade_body AS trade_body \n" +
                        " , oi.session_id AS session_id \n" +
                        " , oi.province_id AS province_id \n" +
                        " , oi.create_date AS create_date \n" +
                        " , ol.source_id AS source_id \n" +
                        " FROM order_detail AS od \n" +
                        " INNER JOIN order_info AS oi \n" +
                        " ON od.order_id = oi.id \n" +
                        " Left JOIN order_log As ol \n" +
                        " ON oi.id = ol.session_id \n"
        );

        resultTable.execute().print();

        // 创建 Kafka upsert
        streamTableEnv.executeSql(
                " CREATE TABLE " + TOPIC_DWD_TRADE_ORDER_DETAIL + " ( \n" +
                        " id STRING \n" +
                        " , course_id STRING \n" +
                        " , course_name STRING \n" +
                        " , order_id STRING \n" +
                        " , user_id STRING \n" +
                        " , origin_amount STRING \n" +
                        " , coupon_reduce STRING \n" +
                        " , final_amount STRING \n" +
                        " , create_time STRING \n" +
                        " , create_date STRING \n" +
                        " , ts STRING \n" +
                        " , user_id STRING \n" +
                        " , out_trade_no STRING \n" +
                        " , trade_body STRING \n" +
                        " , session_id STRING \n" +
                        " , province_id STRING \n" +
                        " , create_date STRING \n" +
                        " , source_id STRING \n" +
                        " , PRIMARY KEY (id) NOT ENFORCED \n" +
                        " ) " + FlinkSqlUtil.getUpersertKafkaSinkDDL( TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        // 写出到 Kafka
        resultTable.executeInsert( TOPIC_DWD_TRADE_ORDER_DETAIL );

    }
}
