package com.atguigu.edu.realtime.dws.app;

/* *
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：16:38
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class DwsTrafficPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficPageViewWindowApp()
                .start(10023,3,"dws_traffic_page_view_window_app", TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        // ds.print();
        // {
        //     "common": {
        //             "sc": "2",
        //             "ar": "20",
        //             "uid": "751",
        //             "os": "iOS 13.3.1",
        //             "ch": "Appstore",
        //             "is_new": "0",
        //             "md": "iPhone Xs Max",
        //             "mid": "mid_314",
        //             "vc": "v2.1.132",
        //             "ba": "iPhone",
        //             "sid": "84b9060a-1d48-43a6-a7d6-f27571a5bb76"
        // },
        //     "page": {
        //     "page_id": "course_list",
        //             "item": "数据库",
        //             "during_time": 17962,
        //             "item_type": "keyword",
        //             "last_page_id": "home"
        // },
        //     "ts": 1736155411183
        // }
    }
}
