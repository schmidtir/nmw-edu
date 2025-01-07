package com.atguigu.edu.realtime.dwd.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dwd.app
 * Author : Kevin
 * Create Date ：2025/1/7
 * Create Time ：19:01
 * TODO 交易域支付成功事务事实表
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

public class DwdTradePaySusDetailApp extends BaseSqlApp {

    public static void main(String[] args) throws Exception {
        new DwdTradePaySusDetailApp()
                .start(10018, 3,"dwd_trade_pay_sus_detail_app");
    }
    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env)  {
        // 设置TTL
        streamTableEnv.getConfig().setIdleStateRetention( Duration.ofSeconds( 15 ) );

        // 过滤业务数据
        readOdsTopicDb( streamTableEnv, "dwd_trade_pay_sus_detail_app_db" );

        // 过滤支付成功数据

        Table paymentInfoTable = streamTableEnv.sqlQuery(
                " SELECT \n" +
                        "    `data`['id'] AS id \n" +
                        " , `data`['out_trade_no'] AS out_trade_no \n" +
                        " , `data`['order_id'] AS order_id \n" +
                        " , `data`['alipay_trade_no'] AS alipay_trade_no \n" +
                        " , `data`['total_amount'] AS total_amount \n" +
                        " , `data`['trade_body'] AS trade_body \n" +
                        " , `data`['payment_type'] AS payment_type \n" +
                        " , `data`['payment_status'] AS payment_status \n" +
                        " , `data`['create_time'] AS create_time \n" +
                        " , date_format( `data`['create_time'], 'yyyy-MM-dd' ) AS create_date \n" +
                        " , `data`['callback_time'] AS callback_time \n" +
                        " , `ts` AS ts \n" +
                        " FROM topic_db \n" +
                        " WHERE `database` = '" + DB_NAME + "'  \n" +
                        " AND `table` = 'payment_info' \n" +
                        " AND `type` = '" + MAXWELL_TYPE_INSERT + "' " // 应选择 update 数据，而模拟出的数据中只有 insert，先用 insert 测试
        );

        // paymentInfoTable.execute().print();
        streamTableEnv.createTemporaryView("payment_info", paymentInfoTable);

        // 读取 dwd_trade_order_detail 下单详情
        streamTableEnv.executeSql(
                " CREATE TABLE dwd_trade_order_detail_temp ( \n" +
                        "  id STRING \n" +
                        " , course_id STRING \n" +
                        " , course_name STRING \n" +
                        " , order_id STRING \n" +
                        " , user_id STRING \n" +
                        " , origin_amount STRING \n" +
                        " , coupon_reduce STRING \n" +
                        " , final_amount STRING \n" +
                        " , session_id STRING \n" +
                        " , province_id STRING \n" +
                        " , source_id STRING " +
                        " ) " + FlinkSqlUtil.getKafkaSourceDDL( "dwd_trade_order_detail", "dwd_trade_pay_sus_detail_app_od" )
        );

        // 关联两张表
        Table resultTable = streamTableEnv.sqlQuery(
                " SELECT \n" +
                        "  pi.id \n" +
                        " , pi.out_trade_no \n" +
                        " , pi.order_id \n" +
                        " , pi.alipay_trade_no \n" +
                        " , pi.total_amount \n" +
                        " , pi.trade_body \n" +
                        " , pi.payment_type \n" +
                        " , pi.payment_status \n" +
                        " , pi.create_time \n" +
                        " , pi.create_date \n" +
                        " , pi.callback_time \n" +
                        " , od.course_id \n" +
                        " , od.course_name \n" +
                        " , od.user_id \n" +
                        " , od.origin_amount \n" +
                        " , od.coupon_reduce \n" +
                        " , od.final_amount \n" +
                        " , od.session_id \n" +
                        " , od.province_id \n" +
                        " , od.source_id \n" +
                        " , pi.ts \n" +
                        " FROM payment_info pi \n" +
                        " LEFT JOIN dwd_trade_order_detail_temp od \n" +
                        " ON pi.order_id = od.order_id"
        );

        // resultTable.execute().print();


        // upsert写入kafka
        streamTableEnv.executeSql(
                " CREATE TABLE " + TOPIC_DWD_TRADE_PAY_SUS_DETAIL + " ( \n" +
                        " id STRING \n" +
                        " , out_trade_no STRING \n" +
                        " , order_id STRING \n" +
                        " , alipay_trade_no STRING \n" +
                        " , total_amount STRING \n" +
                        " , trade_body STRING \n" +
                        " , payment_type STRING \n" +
                        " , payment_status STRING \n" +
                        " , create_time STRING \n" +
                        " , create_date STRING \n" +
                        " , callback_time STRING \n" +
                        " , course_id STRING \n" +
                        " , course_name STRING \n" +
                        " , user_id STRING \n" +
                        " , origin_amount STRING \n" +
                        " , coupon_reduce STRING \n" +
                        " , final_amount STRING \n" +
                        " , session_id STRING \n" +
                        " , province_id STRING \n" +
                        " , source_id STRING \n" +
                        " , ts BIGINT " +
                        " , PRIMARY KEY (id) NOT ENFORCED \n" +
                        " ) " + FlinkSqlUtil.getUpersertKafkaSinkDDL( TOPIC_DWD_TRADE_PAY_SUS_DETAIL )
        );
        resultTable.executeInsert( TOPIC_DWD_TRADE_PAY_SUS_DETAIL );
    }
}
