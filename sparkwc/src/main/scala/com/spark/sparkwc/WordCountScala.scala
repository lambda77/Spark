package com.spark.sparkwc

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {


  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
                    .setAppName("WordCountScala")
                    .setMaster("local")
//                    .set("spark.local.dir", "D://sparkjar//tmp/")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///D://data//wc.txt")

    val rdd = lines.flatMap(_.split(","))
                   .map((_,1))
                   .reduceByKey(_+_)
                   .sortBy(_._2,false)
    rdd.foreach(println)
  }

}
