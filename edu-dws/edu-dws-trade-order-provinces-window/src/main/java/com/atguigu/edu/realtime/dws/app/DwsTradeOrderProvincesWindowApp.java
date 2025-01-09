package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：9:41
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeOrderProvincesBean;
import com.atguigu.edu.realtime.common.bean.TradeOrderSourcesBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTradeOrderProvincesWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderProvincesWindowApp()
                .start(10032,3, "dws_trade_order_provinces_window_app", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {


        ds.print();
        SingleOutputStreamOperator<TradeOrderProvincesBean> filteDs = ds.flatMap(
                new FlatMapFunction<String, TradeOrderProvincesBean>() {
                    @Override
                    public void flatMap(String value, Collector<TradeOrderProvincesBean> out) throws Exception {

                        try {
                            if (
                                    value != null && !value.equals("")
                                            && JSONObject.parseObject(value).getString("user_id") != null
                                            && JSONObject.parseObject(value).getString("source_id") != null
                            ) {
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                String orderId = jsonObject.getString("order_id");
                                String provinceId = jsonObject.getString("province_id");
                                String userId = jsonObject.getString("user_id");
                                BigDecimal finalAmount = jsonObject.getBigDecimal("final_amount");
                                Long ts = jsonObject.getLong("ts") * 1000L;

                                TradeOrderProvincesBean tradeOrderProvincesBean =
                                        TradeOrderProvincesBean
                                                .builder()
                                                .stt("")
                                                .edt("")
                                                .curDate("")
                                                .provinceId(provinceId)
                                                .provinceName("")
                                                .userId(userId)
                                                .orderTotalAmount(finalAmount)
                                                .orderUuCount(0L)
                                                .orderCount(0L)
                                                .ts(ts)
                                                .build();

                                out.collect( tradeOrderProvincesBean );
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("");
                        }
                    }
                }
        );
    }
}
