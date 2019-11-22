package com.spark.sparkwc;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
java7 编写spark word count 程序
 */
public class WordCountJava7 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava7");
        conf.setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 读取本地文件
        JavaRDD<String> lines = jsc.textFile("file:///D://data//wc.txt");
        // 打印出每行数据
        lines.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // 每行数据按照逗号分隔
        JavaRDD<String> rdd1 = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] arr = s.split(",");
                List<String> list = Arrays.asList(arr);
                return list.iterator();
            }
        });

        // 把每个单词拼成一个二元组（word，1）
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(s, 1);
                return tuple2;
            }
        });
        // 每个单词进行统计
        JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 打印结果
        rdd3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
        jsc.close();

    }

}
