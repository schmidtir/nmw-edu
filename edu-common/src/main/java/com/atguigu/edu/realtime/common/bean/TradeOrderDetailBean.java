package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：14:37
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.annotation.JSONField;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeOrderDetailBean {

    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 下单独立用户数
    Long orderUvCount;

    // 新增下单用户数
    Long newOrderUserCount;

    @JSONField(serialize=false)
    Long ts;

}
