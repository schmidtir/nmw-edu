package com.atguigu.edu.realtime.common.util;

import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;



public class JdbcUtil {

    public static java.sql.Connection getConnection(){
        try {
            Class.forName(Constant.MYSQL_DRIVER);//注册驱动

            //获取连接
            java.sql.Connection connection = DriverManager.getConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD
            );
            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("获取连接失败");
        }

    }
    //关闭连接的方法
    public static void closeConnection(Connection connection){
        try {
            if(connection != null && !connection.isClosed()){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("关闭mysql连接失败");
        }
    }

    /**
     * 查询多条数据
     */

    public static <T> List<T> queryList(Connection connection,String  sql , Class<T> returnType,Boolean isUnderScoreToCamel){
        List<T> configList = new ArrayList<>();
        try {
            //编译sql
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet resultSet = preparedStatement.executeQuery();//执行sql
            //如果执行的是查询，调用executeQuery，如果是增删改，调用executeUpdate

            //结果集元数据对象
            ResultSetMetaData metaData = resultSet.getMetaData();

            //结果集列的个数
            int columnCount = metaData.getColumnCount();



            //处理结果集(要求：结果集的列要跟对象的属性对应起来，要么完全一样，要么遵循下划线和驼峰的映射规则)
            while (resultSet.next()) {

                //创建T对象
                T t = returnType.newInstance(); //要求T类中提供无参构造器

                //获取每个列的名字
                for (int i = 1; i <= columnCount; i++) {
                    //这里从1开始，是因为jdbc的下标都是从1开始的

                    // metaData.getColumnName();//不支持别名
                    //获取每个列的名字
                    String columnName = metaData.getColumnLabel(i);//支持别名
                    //i是指示第几列，从1开始

                    //获取列对应的值
                    Object columnValue = resultSet.getObject(columnName);

                    //约定（写通用的操作一定要写约定）：结果集的列名和对象的属性名一致（要么完全一样，要么遵循下划线和驼峰的映射规则）
                    //比如：   结果集的列名：source_table     对象的属性名：source_table
                    //        结果集的列名：source_table     对象的属性名：sourceTable
                    //赋值到对象的属性上
                    if(isUnderScoreToCamel){
                        //如果满足isUnderScoreToCamel这个条件，就转换，否则不转
                        //下划线转驼峰  比如：source_table  转成  sourceTable
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    //通过反射机制，获取对象的属性
                    //1.基于属性的方式:
//                    Field field = returnType.getDeclaredField(columnName);//returnType获取声明的属性
//                    field.setAccessible(true);//支持访问私有属性
//                    field.set(t,columnValue);//根据获取的属性对象往当前T对象的属性上赋值


                    //2.基于属性对应的set方法的方式:
                    //比如；属性名: String  sourceTable  ;    set方法: public void  setSourceTable( String sourceTable){ }
//                    Method method = returnType.getDeclaredMethod("setSourceTable", returnType.getDeclaredField(columnName).getType());//returnType获取声明的方法
//                    method.invoke(t,columnValue);


                    // 使用第三方的工具来完成
                    BeanUtils.setProperty( t , columnName , columnValue);
                }
                configList.add(t);//每处理完一行数据，就往集合中添加一个对象

            }
            //关闭连接
            resultSet.close();
            preparedStatement.close();

            return  configList;


        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("查询mysql数据失败");
        }

    }

    /**
     * 查询单条数据
     */
    public static <T> T querOne(Connection connection,String sql,Class<T> returnType,Boolean isUnderScoreToCamel){

        List<T> configList = queryList(connection,sql,returnType,isUnderScoreToCamel);

        if(configList.size() > 0){//如果集合的长度大于0，就返回集合中的第一个对象
            return configList.get(0);
        }
        return null;//如果集合的长度不大于0，表示没有查到数据
    }


    public static void main(String[] args) {
        Connection connection = getConnection();

        String sql =
                " select " +
                        " source_table ,sink_table , sink_family , sink_columns , sink_row_key " +
                        " from edu_config.table_process_dim " +
                        " where source_table = 'base_province'" ;
        TableProcessDim tableProcessDims = querOne(connection, sql, TableProcessDim.class, true);
        System.out.println("tableProcessDims = " + tableProcessDims);

        closeConnection(connection);
    }


}



