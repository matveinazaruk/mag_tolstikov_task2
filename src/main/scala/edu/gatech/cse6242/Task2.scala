package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {

    System.out.println("******************************")
    // return;
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    // read the file
    val file = sc.textFile("hdfs://quickstart.cloudera:8020/" + args(0))

    /* TODO: Needs to be implemented */
 
    val splitted = file.map(line => (line.split("\t")(1), line.split("\t")(2).toInt))
 
    // count the occurrence of each word
    val graphCounts = splitted.reduceByKey(_ + _)
  	
    // store output on given HDFS path.
    // YOU NEED TO CHANGE THIS
    graphCounts.map(c => c._1 + "\t" + c._2).saveAsTextFile("hdfs://localhost:8020" + args(1))
    // System.out.println(graphCounts.collect().mkString(", "))
  }
}
