package edu.realtime.dwd.app;

import com.atguigu.edu.realtime.common.base.BaseSqlApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.FlinkSqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.shell.Concat;

import java.time.Duration;

/**
 * Package Name: edu.realtime.dwd.app
 * Author: WZY
 * Create Date: 2025/1/8
 * Create Time: 上午11:11
 * Vserion : 1.0
 * TODO
 */
public class DwdTrafficUserRegister extends BaseSqlApp {
    public static void main(String[] args) throws Exception {
        new DwdTrafficUserRegister().start(10016 , 3 , "dwd_user_register");
    }
    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception {
        streamTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));

        streamTableEnv.executeSql("CREATE TABLE page_log (\n" +
                " `common` map<string,string>,\n" +
                " `page` string,\n" +
                " `ts` string\n" +
                ")" + FlinkSqlUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE , "dwd_user_register")
        );

        // streamTableEnv.executeSql("select * from page_log").print();

        FlinkSqlUtil.createTopicDb(streamTableEnv , "dwd_user_register");
        // 过滤用户表数据
        Table userRegister = streamTableEnv.sqlQuery("select \n" +
                " data['id'] id , \n" +
                " data['create_time'] create_time , \n " +
                " date_format(data['create_time'] , 'yyyy-MM-dd') create_date, \n" +
                " ts \n" +
                " from topic_db \n" +
                " where `table`='user_info'\n" +
                " and `type`='insert'" +
                ""
        );
        // userRegister.execute().print();
        streamTableEnv.createTemporaryView("user_register" , userRegister);
        // 过滤注册日志的维度信息
        Table dimLog = streamTableEnv.sqlQuery(
                "select \n" +
                        " common['uid'] user_id ,\n" +
                        " common['ch'] channel ,\n" +
                        " common['ar'] province_id ,\n" +
                        " common['vc'] version_code,\n" +
                        " common['sc'] source_id,\n" +
                        " common['mid'] mid_id,\n" +
                        " common['ba'] brand ,\n" +
                        " common['md'] model ,\n" +
                        " common['os'] operate_system \n" +
                        " from page_log\n" +
                        " where common['uid'] is not null \n"
        );
        // dimLog.execute().print();
        streamTableEnv.createTemporaryView("dim_log" , dimLog);
        // join两张表格
        Table resultTable = streamTableEnv.sqlQuery(
                "select \n" +
                        " ur.id user_id,\n" +
                        " create_time register_time,\n" +
                        " create_date register_date,\n" +
                        " channel,\n" +
                        " province_id,\n" +
                        " version_code,\n" +
                        " source_id,\n" +
                        " mid_id,\n" +
                        " brand ,\n" +
                        " model ,\n" +
                        " operate_system,\n" +
                        " ts ,\n" +
                        " current_row_timestamp() row_op_ts \n" +
                        " from user_register ur \n" +
                        " left join dim_log dl\n" +
                        " on ur.id=dl.user_id"
        );
        // resultTable.execute().print();
        streamTableEnv.createTemporaryView("result_table" , resultTable);
        streamTableEnv.executeSql(
                "create table dwd_user_register(\n" +
                        " user_id string,\n" +
                        " register_time string,\n" +
                        " register_date string,\n" +
                        " channel string,\n" +
                        " province_id string,\n" +
                        " version_code string,\n" +
                        " source_id string,\n" +
                        " mid_id string,\n" +
                        " brand string ,\n" +
                        " model string ,\n" +
                        " operate_system string,\n" +
                        " ts string ,\n" +
                        " row_op_ts TIMESTAMP_LTZ(3), \n" +
                        " PRIMARY KEY (user_id) NOT ENFORCED \n" +
                        ")" + FlinkSqlUtil.getUpersertKafkaSinkDDL(Constant.TOPIC_DWD_USER_REGISTER)
        );
        streamTableEnv.executeSql("insert into dwd_user_register " + "select * from result_table");
    }
}
