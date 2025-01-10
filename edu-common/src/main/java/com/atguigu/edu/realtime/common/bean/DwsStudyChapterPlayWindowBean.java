package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author: WZY
 * Create Date: 2025/1/9
 * Create Time: 下午8:40
 * Vserion : 1.0
 * TODO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsStudyChapterPlayWindowBean {

    // 窗口起始时间
    String stt ;
    // 窗口结束时间
    String edt;
    // 视频 ID
    @JSONField(serialize=false)
    String videoId;

    // 章节 ID
    String chapterId;

    // 章节名称
    String chapterName;

    // 用户 ID
    @JSONField(serialize=false)
    String userId;

    // 播放次数
    Long playCount;

    // 播放总时长
    Long playTotalSec;

    // 观看人数
    Long playUuCount;

    // 时间戳
    Long ts;
}
