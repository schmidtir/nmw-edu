package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseSqlApp;
import com.atguigu.edu.realtime.common.bean.KeywordBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.SplitKeyWordTableFunction;
import com.atguigu.edu.realtime.common.util.FlinkSqlUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Package Name: com.atguigu.edu.realtime.dws.app
 * Author: WZY
 * Create Date: 2025/1/8
 * Create Time: 下午3:53
 * Vserion : 1.0
 * TODO
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021 , 3 , Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }

    @Override
    protected void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception {
        streamTableEnv.executeSql(
                "create table page_log(\n" +
                        " `common` map<string , string> ,\n" +
                        " `page` map<string , string> ,\n" +
                        " `ts` bigint , \n" +
                        " row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000 , 'yyyy-MM-dd HH:mm:ss')),\n" +
                        " WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND \n" +
                        " ) " + FlinkSqlUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE , Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW)
        );
        // streamTableEnv.sqlQuery("select * from page_log").execute().print();
        // 从表中搜索过滤行为
        Table searchTable = streamTableEnv.sqlQuery(
                "select \n" +
                        " page['item'] full_word , \n" +
                        " row_time \n" +
                        " from page_log \n" +
                        " where page['item'] is not null\n" +
                        " and page['item_type'] = 'keyword'"
        );
        //searchTable.execute().print();
        streamTableEnv.createTemporaryView("search_table" , searchTable);
        // 对搜索内容进行分词
        streamTableEnv.createTemporaryFunction("ik_analyze" , SplitKeyWordTableFunction.class);
        Table splitTable = streamTableEnv.sqlQuery(
                "select\n" +
                        " keyword , \n" +
                        " row_time  \n" +
                        " from search_table,\n" +
                        " lateral table(ik_analyze(full_word)) \n" +
                        " as t(keyword)"
        );
        //splitTable.execute().print();
        streamTableEnv.createTemporaryView("split_table" , splitTable);
        Table result = streamTableEnv.sqlQuery(
                "select\n" +
                        " DATE_FORMAT( window_start , 'yyyy-MM-dd HH:mm:ss') stt ,\n" +
                        " DATE_FORMAT( window_end , 'yyyy-MM-dd HH:mm:ss') edt , \n" +
                        " DATE_FORMAT (window_start , 'yyyy-MM-dd') cur_date ,\n'" +
                        Constant.KEYWORD_SEARCH + "' source,\n" +
                        " keyword , \n" +
                        " count(*) keyword_count ,\n" +
                        " UNIX_TIMESTAMP()*100 ts\n" +
                        " from \n" +
                        " table ( " +
                        " tumble ( table split_table , DESCRIPTOR(row_time) , INTERVAL '10' SECOND )" +
                        " ) " +
                        " GROUP BY window_start, window_end , keyword"
        );
        streamTableEnv.createTemporaryView("result_table" , result);
        // result.execute().print();
        Table result_nots = streamTableEnv.sqlQuery(
                "select \n" +
                        " stt , edt , cur_date , source , keyword , keyword_count "
                        + " from result_table"
        );
        // result_nots.execute().print();
        streamTableEnv.executeSql(
                "create table " + Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW  + "(\n" +
                        " stt string ,\n" +
                        " edt string , " +
                        " cur_date string ," +
                        " source string ," +
                        " keyword string ," +
                        " keyword_count bigint " +
                        " ) " + FlinkSqlUtil.getDorisSinkDDL(Constant.DORIS_DB_NAME , Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW)

        );
        result_nots.executeInsert( Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);

    }
}
