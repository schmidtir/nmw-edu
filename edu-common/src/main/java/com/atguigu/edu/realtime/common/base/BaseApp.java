package com.atguigu.edu.realtime.common.base;

/* *
 * Package Name: com.atguigu.edu.realtime.common.base
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：19:57
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.edu.realtime.common.constant.Constant.*;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    /***
     * 数据处理的方法
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds);


    /***
     *  抽取通用方法作为模板，进行复用
     *
     * @param port
     *              测试环境下启动本地 WebUI 的端口，为了避免本地端口冲突，做出以下规定
     *                  1) DIM 层维度分流应用使用 10001 端口
     *                  2) DWD 层应用程序按照在本文档出现的先后顺序，端口从 10011 开始，自增 +1
     *                  3) DWS 层应用程序按照在本文档出现的先后顺序，端口从 10021 开始，自增 +1
     * @param parallelism
     *              默认 3 个并行度，对应 Kafka 的分区数，也可以按照实际情况灵活调整
     * @param ckAndgroupId
     *              消费Kafka 主题时的消费者组 ID和检查点路径的最后一级目录名称，二者取值相同
     *              为 Job 主程序类名的下划线命名形式。 如 DimAPP 的改参数取值为 dim_app
     * @param topic
     *              消费的主题
     * @throws Exception
     */
    public void start(Integer port, Integer parallelism, String ckAndgroupId, String topic)  {
        //
        System.setProperty( HDFS_USER_NAME_CONFIG, HDFS_USER_NAME_VALUE );

        //
        Configuration conf = new Configuration();
        conf.setInteger( RestOptions.PORT, port );

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment( conf );
        env.setParallelism(parallelism);

        // 2. 检查点配置
//        env.setStateBackend( new HashMapStateBackend() );
        env.enableCheckpointing(10000L);

        env.getCheckpointConfig().setCheckpointingMode( CheckpointingMode.EXACTLY_ONCE );

//        env.getCheckpointConfig().setCheckpointStorage( CK_PATH_PREFIX + ckAndgroupId );
//
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//
//        env.getCheckpointConfig().setCheckpointTimeout(100000);
//
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup( RETAIN_ON_CANCELLATION );

        // 3. 配置数据源
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(ckAndgroupId, topic);


        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        // 4. 对数据进行转换处理
        handle( env, ds );


        // 5. 启动执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
