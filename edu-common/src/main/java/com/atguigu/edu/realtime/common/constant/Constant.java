package com.atguigu.edu.realtime.common.constant;

/* *
 * Package Name: com.atguigu.gmall.realtime.common.constant
 * Author : Kevin
 * Create Date ：2024/12/20
 * Create Time ：11:43
 * <p>
 * version: 0.0.1.0
 */


public class Constant {


    public static final String MYSQL_URL = "jdbc:mysql://hadoop101:3306?useSSL=false";
    public static final String REDIS_HOST = "hadoop103";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DIM_EX_ONE_DAY = 24 * 60 * 60 * 1000;


    public static final String MAXWELL_TYPE_DELETE = "delete";
    public static final String MAXWELL_TYPE_INSERT = "insert";
    public static final String MAXWELL_TYPE_UPDATE = "update";
    public static final String MAXWELL_TYPE_BOOTSTRAP_INSERT = "bootstrap-insert";

    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop103,hadoop104,hadoop105";

    public static final String HBASE_NAMESPACE_DIM = "edu_realtime";

    public static final String HBASE_NAMESPACE = "edu_realtime";

    public static final String CDC_OP_C = "c";
    public static final String CDC_OP_D = "d";
    public static final String CDC_OP_R = "r";
    public static final String CDC_OP_U = "u";
    public static final String DB_NAME = "edu";
    public static final String CONFIG_DB_NAME = "edu_config";
    public static final String TABLE_PROCESS_DIM = "table_process_dim";
    public static final String TABLE_PROCESS_DWD = "table_process_dwd";


    public static final String CK_PATH_PREFIX = "hdfs://hadoop101:8020/edu/stream/";

    public static final String HDFS_USER_NAME_CONFIG = "HADOOP_USER_NAME";
    public static final String HDFS_USER_NAME_VALUE = "atguigu";

    public static final String DORIS_FENODES = "hadoop102:7030";
    public static final String DORIS_DB_NAME = "edu_realtime";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "000000";

    public static final String KAFKA_BROKERS = "hadoop105:9092,hadoop103:9092,hadoop104:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "hadoop101";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL_CONFIG = "jdbc:mysql://hadoop101:3306/edu_config";

    public static final String TOPIC_DWD_TRAFFIC_DIRTY = "dwd_traffic_dirty";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_APPVIDEO = "dwd_traffic_appvideo";

    public static final String TOPIC_DWD_TRAFFIC_UNI_VISITOR_DETAIL = "dwd_traffic_uni_visitor_detail";
    public static final String TOPIC_DWD_TRAFFIC_USER_JUMP = "dwd_traffic_user_jump";
    public static final String TOPIC_DWD_STUDY_PLAY = "dwd_study_play";
    public static final String TOPIC_DWD_USER_LOGIN = "dwd_user_login";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_PAY_SUS_DETAIL = "dwd_trade_pay_sus_detail";


    public static final String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";

}
