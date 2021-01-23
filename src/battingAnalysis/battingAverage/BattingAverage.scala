package battingAverage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BattingAverage {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount");
    //create spark context object
    var sc = new SparkContext(conf);
    //loading a file into and spliting csv file
    var IPLRdd = sc.textFile("/user/input/IPLMatchesBallByBall.csv", 5).map(_.split(","));

    //filter records from 2015 and innings 1 and 2 . 3rd and 4th innings comes under super over
    var IPLFilterRdd = IPLRdd.filter(record => record(14).toInt >= 2015 && record(1).toInt < 3);

    //calculating and filtering batsmen who has scored more than 300 runs
    var batsmenRuns = IPLFilterRdd.map(record => (record(4), record(7).toDouble)).reduceByKey(_ + _).filter(_._2 > 300);

    //counting outs of all batsmen
    var batsmenOuts = IPLFilterRdd.filter(_(10).nonEmpty).map(record => (record(10), 1)).reduceByKey(_ + _);

    var joinedData = batsmenRuns.join(batsmenOuts);
    
    //getting output as (bastman , BattingAverage) in descending order with average upto two decimals
    var resultData = joinedData.coalesce(1).map(r => (r._1, (((r._2._1 / r._2._2) * 100).toInt).toDouble / 100)).sortBy(r => r._2, ascending = false);
    
    //    resultData.foreach(println);
    //    resultData.saveAsTextFile("/user/output/spark/battingAVerage");
  }
}
