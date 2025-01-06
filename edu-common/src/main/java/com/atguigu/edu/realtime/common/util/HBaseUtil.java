package com.atguigu.edu.realtime.common.util;

/* *
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：17:00
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


public class HBaseUtil {



    /***
     * 获取异步连接
     */
    public static AsyncConnection getAsyncConnection()  {
        // 1. 创建配置对象
        Configuration conf = HBaseConfiguration.create();

        // 2. 添加配置参数
        conf.set("hbase.zookeeper.quorum",HBASE_ZOOKEEPER_QUORUM );

        // 3. 创建hbase的连接
        // 默认使用同步连接
        AsyncConnection asyncConnection;
        try {
            asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取 HBase AsyncConnection 失败");
        }
        return asyncConnection;
    };

    /***
     * 获取连接
     */
    public static Connection getConnection()  {
        // 1. 创建配置对象
        Configuration conf = HBaseConfiguration.create();

        // 2. 添加配置参数
        conf.set("hbase.zookeeper.quorum",HBASE_ZOOKEEPER_QUORUM );

        // 3. 创建hbase的连接
        // 默认使用同步连接
        Connection connection;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("获取 HBase Connection 失败");
        }
        return connection;
    };

    /**
     * 关闭连接
     */
    public static void closeConnection(Connection connection) {

        if( connection != null || !connection.isClosed() ) {

            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("关闭 HBase Connection 失败");
            }
        }
    };
    /**
     * 关闭连接
     */
    public static void closeAsyncConnection(AsyncConnection asyncConnection) {

        if( asyncConnection != null || !asyncConnection.isClosed() ) {

            try {
                asyncConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("关闭 HBase Connection 失败");
            }
        }
    };

    /***
     *
     * @param connection    HBase 连接
     * @param namespaceName 表空间
     * @param tableName     表名
     * @param cfs           列族名
     */
    public static void createTable( Connection connection, String namespaceName, String tableName, String  cfs ) {
        Admin admin = null;
        // 获取 Admin 对象
        try {
            admin = connection.getAdmin();

            TableName tn = TableName.valueOf( namespaceName, tableName );
            // 判断表是否存在
            if (admin.tableExists(tn)) {
                System.out.println( " HBase table " + namespaceName + "." + tableName + " already exists!");
                return;
            }

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn)
                    .setColumnFamily(

                            ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(cfs))
                    )
                    .build();

            admin.createTable(tableDescriptor);

            System.out.println( " HBase table " + namespaceName + "." + tableName + " created! " );

            admin.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("HBase建表失败");
        }finally {
            if( admin != null ){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException( "关闭 Admin 失败 " );

                }
            }

        }
    }

    public static void dropTable(Connection connection, String namespaceName, String tableName){
        Admin admin = null;
        // 获取 Admin 对象
        try {
            admin = connection.getAdmin();

            TableName tn = TableName.valueOf(namespaceName,tableName);
            // 判断表是否存在
            if ( !admin.tableExists( tn ) )  {
                System.out.println( " HBase table " + namespaceName + "." + tableName + " not exists!");
                return;
            }

            // 先 disable
            admin.disableTable( tn );

            // 再 delete
            admin.deleteTable( tn );

            System.out.println( " HBase table " + namespaceName + "." + tableName + " deleted! " );

            admin.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("HBase删表失败");
        }finally {
            if( admin != null ){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException( "关闭 Admin 失败 " );

                }
            }

        }
    }

    /**
     * 写入数据
     * shell: put 'namespace:tableName' , 'rk' , 'cf:cl' , 'value'
     */
    public static void putRow(Connection connection, String namespaceName, String tableName, String rowKey,String cf, JSONObject dataJson) throws IOException {

        try {
            TableName tn = TableName.valueOf( namespaceName, tableName );
            Table table = connection.getTable( tn );

            // 将 dataJson 处理成 put 集合
            List<Put> putList = dataJson
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null) // 过滤空值
                    .map(entry -> {
                        Put put = new Put( Bytes.toBytes(rowKey) );
                        put.addColumn( Bytes.toBytes(cf), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
                        return put;
                    })
                    .collect(Collectors.toList());

            table.put( putList );

            System.out.println( " ➕ HBase 成功写入 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据。" );
            table.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println( " ⚠️ HBase 写入 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠️" );
        }
    }

    /**
     * 删除数据
     */
    public static void delRow(Connection connection, String namespaceName, String tableName, String rowKey){

        try {
            TableName tn = TableName.valueOf( namespaceName, tableName );
            Table table = connection.getTable( tn );

            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println( " ➖ HBase 成功删除 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据。" );

            table.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println( " ⚠️ HBase 删除 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠️" );

        }
    }

    /**
     * 查询数据
     */
    public static JSONObject getRow(Connection connection, String namespaceName, String tableName, String rowKey ){

        try {
            TableName tn = TableName.valueOf( namespaceName, tableName );
            Table table = connection.getTable( tn );
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            // 将查询结果封装成 JSONObject 对象
            JSONObject jsonObject = new JSONObject();

            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String k = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));

                jsonObject.put(k,v);
            }
            table.close();
            System.out.println( "  HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据: " + jsonObject );
            return jsonObject;

        }catch (Exception e){
            e.printStackTrace();
//            System.out.println( " ⚠️ HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠️" );
//            closeConnection(connection);
//            return null;// 🐞🐞
            throw new RuntimeException("  ⚠\uFE0F HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠\uFE0F");
        }
    }
    /**
     * 查询数据
     */
    public static JSONObject getRowAsync(AsyncConnection asyncConnection, String namespaceName, String tableName, String rowKey ){

        try {
            TableName tn = TableName.valueOf( namespaceName, tableName );
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConnection.getTable(tn);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();

            // 将查询结果封装成 JSONObject 对象
            JSONObject jsonObject = new JSONObject();

            List<Cell> cells = result.listCells();

            for (Cell cell : cells) {
                String k = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));

                jsonObject.put(k,v);
            }
//            asyncTable.close();
            System.out.println( "  HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据: " + jsonObject );
            return jsonObject;

        }catch (Exception e){
            e.printStackTrace();
//            System.out.println( " ⚠️ HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠️" );
//            closeConnection(connection);
//            return null;// 🐞🐞
            throw new RuntimeException("  ⚠\uFE0F HBase 查询 " + namespaceName + "." + tableName + " 表，rowkey 为 " + rowKey + " 的数据失败！⚠\uFE0F");
        }
    }

    public static void main(String[] args) {
        AsyncConnection asyncConnection = getAsyncConnection();
        JSONObject gmallRealtime = getRowAsync(asyncConnection, "gmall_realtime", "", "1");
        System.out.println(gmallRealtime);
        closeAsyncConnection(asyncConnection);
    }
}
