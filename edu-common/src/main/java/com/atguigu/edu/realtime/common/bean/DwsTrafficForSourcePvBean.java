package com.atguigu.edu.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author: WZY
 * Create Date: 2025/1/9
 * Create Time: 下午2:23
 * Vserion : 1.0
 * TODO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTrafficForSourcePvBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 版本号
    String versionCode;
    // 来源 ID
    String sourceId;
    // 来源名称
    String sourceName;
    // 省份 ID
    String provinceId;
    // 省份名称
    String provinceName;
    // 新老访客标记
    String isNew;
    // 独立访客数
    Long uvCount;
    // 会话总数
    Long totalSessionCount;
    // 页面浏览数
    Long pageViewCount;
    // 页面总停留时长
    Long totalDuringTime;
    // 跳出会话数
    Long jumpSessionCount;
    // 时间戳
    Long ts;
}
