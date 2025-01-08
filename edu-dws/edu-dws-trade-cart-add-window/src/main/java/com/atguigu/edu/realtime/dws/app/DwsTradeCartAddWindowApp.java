package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：8:44
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTradeCartAddWindowApp extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeCartAddWindowApp()
                .start(10027,3, "dws_trade_cart_add_window_app", TOPIC_DWD_TRADE_CART_ADD );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // 读取过滤 dwd加购主题数据

        // 按照 user_id分组

        // 设置水位线

        // 开窗聚合

        // 数据写入doris

    }
}
