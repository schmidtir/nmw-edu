package com.atguigu.edu.realtime.common.base;

/* *
 * Package Name: com.atguigu.edu.realtime.common.base
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：19:58
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.util.FlinkSqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public abstract class BaseSqlApp {

    public void start(Integer port, int parallelism, String ckPath ) throws Exception {
        System.setProperty(HDFS_USER_NAME_CONFIG, HDFS_USER_NAME_VALUE);

        //
        Configuration conf = new Configuration();
        conf.setInteger( RestOptions.PORT, port );
//        conf.setString("taskmanager.memory.process.size", "1024m");
//        conf.setString("jobmanager.memory.process.size", "1024m");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment( conf );

        env.setParallelism( parallelism );

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create( env );

        //
//        env.setStateBackend( new HashMapStateBackend() );
        env.enableCheckpointing(5000 );
//
        env.getCheckpointConfig().setCheckpointingMode( CheckpointingMode.EXACTLY_ONCE );
//
//        env.getCheckpointConfig().setCheckpointStorage( CK_PATH_PREFIX + ckPath );
//
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//
//        env.getCheckpointConfig().setCheckpointTimeout(1000000);
//
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup( RETAIN_ON_CANCELLATION );

        handle( streamTableEnv, env );

//        // 5. 启动执行
//        try {
//            env.execute();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    protected abstract void handle(StreamTableEnvironment streamTableEnv, StreamExecutionEnvironment env) throws Exception;

    public static void readOdsTopicDb(StreamTableEnvironment streamTableEnv, String groupId)  {
        // 连接器表
        streamTableEnv.executeSql(
                " CREATE TABLE topic_db ( \n " +
                        " `database` STRING \n " +
                        " , `table` STRING \n " +
                        " , `type` STRING \n " +
                        " , `ts` BIGINT \n " +
                        " , `data` MAP< STRING, STRING > \n " +
                        " , `old` MAP< STRING, STRING > \n " +
                        " , `pt` AS PROCTIME() " +
                        " , `et` AS TO_TIMESTAMP_LTZ( ts, 0) " +
                        " , WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
                        " ) "+ FlinkSqlUtil.getKafkaSourceDDL( TOPIC_DB ,groupId)
        );
    }


    public static void readDimBaseDic(StreamTableEnvironment streamTableEnv)  {
        streamTableEnv.executeSql(
                " CREATE TABLE dim_base_dic ( " +
                        " dic_code STRING \n " +
                        " , info ROW<dic_name STRING> \n " +
                        " , PRIMARY KEY (dic_code) NOT ENFORCED \n " +
                        " ) WITH ( \n " +
                        " 'connector' = 'hbase-2.2' \n " +
                        " , 'table-name' = '" + HBASE_NAMESPACE_DIM + ":dim_base_dic' \n " +
                        " , 'zookeeper.quorum' = '" + HBASE_ZOOKEEPER_QUORUM + "' \n " +
                        " , 'lookup.async' = 'true' \n " +
                        " , 'lookup.cache' = 'PARTIAL' \n " +
                        " , 'lookup.partial-cache.max-rows' = '100' \n " +
                        " , 'lookup.partial-cache.expire-after-write' = '1 hour' \n " +
                        " , 'lookup.partial-cache.expire-after-access' = '1 hour' \n " +
                        " ); "
        );
    }

}
