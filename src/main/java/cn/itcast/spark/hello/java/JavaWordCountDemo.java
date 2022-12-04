package cn.itcast.spark.hello.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 15:25
 * Desc: 演示Java写Spark代码
 */
public class JavaWordCountDemo {
    public static void main(String[] args) {
        // 0. Init Env
        SparkConf conf = new SparkConf().setAppName("JavaWC").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. 读取数据
        JavaRDD<String> file = sc.textFile("data/input/words.txt");
        // 2. 计算
        // 标准的匿名内部类的方式去完成函数式编程(计算逻辑的传入)
//        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.stream(s.split(" ")).iterator();
//            }
//        });
        // 或者通过Java的Lambda表达式的方式去模拟函数式编程的写法
        JavaPairRDD<String, Integer> result = file.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x1, x2) -> x1 + x2);
        System.out.println(result.collect());
    }
}
