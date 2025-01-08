package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：16:05
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
public class TradePaySusWindowBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 支付成功独立用户数
    Long paySucUvCount;

    // 支付成功新用户数
    Long paySucNewUserCount;

    @JSONField(serialize=false)
    Long ts;
}
