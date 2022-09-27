package com.buwenbuhuo.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-09-28 0:59
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 批处理wordcount举例（lamda表达式写法）
 */
public class Chapter01_Batch_wordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataSource<String> dataSource = env.readTextFile("Java_of_Flink-1.13/input/words.txt");

        // 3. 转换数据格式：将每行数据进行分词，组成二元组
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = dataSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            // 将一行文本进行分词
            String[] words = line.split(" ");
            // 将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }// TODO 如果使用lambda表达式，可能因为类型擦除 报错解决： returns（Types.类型）
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> group = wordAndOne.groupBy(0);

        // 5. 分组进行聚合统计
        AggregateOperator<Tuple2<String, Integer>> wordSum = group.sum(1);

        // 6. 打印结果
        wordSum.print();

    }
}
