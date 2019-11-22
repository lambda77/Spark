package com.spark.sparkwc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/*
 java8 实现spark word count统计
 */
public class WordCountJava8 {


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountJava8")
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 读取本地文件
        JavaRDD<String> lines = jsc.textFile("file:///D://data//wc.txt");

        // 切分单词
        JavaRDD<String> rdd1 = lines.flatMap(line -> Arrays.asList(line.split(",")).iterator());
        // 把每个word拼成二元组 (word, 1)
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(word -> new Tuple2<>(word, 1));

        // 进行单词统计
        JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey((x, y) -> x+y);

        // 按照单词数量进行降序排序，先转换k-v，在按照key降序排序
        JavaPairRDD<Integer, String> rdd4 = rdd3.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Integer, String> rdd5 = rdd4.sortByKey(false,1);// 计算后会变为1个分区
        JavaPairRDD<String, Integer> rdd6 = rdd5.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        // 打印结果
        rdd6.foreach(tuple -> System.out.println(tuple));

        jsc.close();

    }



}
