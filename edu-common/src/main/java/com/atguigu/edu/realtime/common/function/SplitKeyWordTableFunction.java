package com.atguigu.edu.realtime.common.function;

import com.atguigu.edu.realtime.common.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Package Name: com.atguigu.edu.realtime.common.function
 * Author: WZY
 * Create Date: 2025/1/8
 * Create Time: 下午5:14
 * Vserion : 1.0
 * TODO
 */
@FunctionHint(output = @DataTypeHint("ROW< keyword string>"))
public class SplitKeyWordTableFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        // 使用 IK 进行分词
        List<String> words = IKUtil.splitWord(keyword);
        // 将分好的每个词包装成 Row 对象 进行输出 TODO 为什么非要包装成 ROW 对象来着
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
