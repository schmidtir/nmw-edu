package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author: WZY
 * Create Date: 2025/1/10
 * Create Time: 上午8:52
 * Vserion : 1.0
 * TODO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsUserLoginWindowBean {
    // 窗口开始结束
    String stt;
    String edt;
    String curDate;
    // 独立用户
    Long uvCount;
    // 回流用户
    Long backCount;
    // 时间戳
    @JSONField(serialize=false)
    Long ts;
}
