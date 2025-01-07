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
                .start( 10017,3, "dwd_trade_order_detail_app");
    }
    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception {

        // 设置TTL
        streamTableEnv.getConfig().setIdleStateRetention( Duration.ofSeconds( 15 ) );

        // 过滤业务数据
        readOdsTopicDb( streamTableEnv, "dwd_trade_order_detail_app" );

        streamTableEnv.executeSql(" SELECT * FROM topic_db ").print();

//        // 读取 dwd_traffic_page_log
//        streamTableEnv.executeSql(
//                " CREATE TABLE page_log ( \n" +
//                        " `common` map<STRING,STRING> \n" +
//                        " , `page` map<STRING,STRING> \n" +
//                        " , `ts` STRING \n" +
//                        " ) " + FlinkSqlUtil.getKafkaSourceDDL( "dwd_traffic_page", "dwd_trade_order_detail_app" )
//        );

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

//        orderDetailTable.execute().print();
        streamTableEnv.createTemporaryView("order_detail", orderDetailTable);

        // 过滤订单表

        Table orderInfoTable = streamTableEnv.sqlQuery(
                " SELECT \n " +
                        "  `data`['id'] \n" +
                        " , `data`['user_id'] \n" +
                        " , `data`['origin_amount'] \n" +
                        " , `data`['coupon_reduce'] \n" +
                        " , `data`['final_amount'] \n" +
                        " , `data`['order_status'] \n" +
                        " , `data`['out_trade_no'] \n" +
                        " , `data`['trade_body']  \n" +
                        " , `data`['session_id']  \n" +
                        " , `data`['province_id']  \n" +
                        " , `data`['create_time']  \n" +
                        " , `data`['expire_time']  \n" +
                        " , date_format( `data`['create_time'], 'yyyy-MM--dd' ) AS create_date \n" +
                        " , ts \n" +
                        " from topic_db \n" +
                        " WHERE `database` = '" + DB_NAME + "'  \n" +
                        " AND `table` = 'order_info' \n" +
                        " AND `type` = '" + MAXWELL_TYPE_INSERT + "' "
        );

        //orderInfoTable.execute().print();
        streamTableEnv.createTemporaryView("order_info", orderInfoTable);


        // 获取下单日志
        // 关联3张表
        // 创建 Kafka upsert
        // 写出到 Kafka

    }
}
