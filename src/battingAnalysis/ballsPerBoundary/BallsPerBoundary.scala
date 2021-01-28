package ballsPerBoundary

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BallsPerBoundary {
  val conf = new SparkConf().setAppName("WordCount");
  //create spark context object
  var sc = new SparkContext(conf);
  //loading a file into and spliting csv file
  var IPLRdd = sc.textFile("/user/input/IPLMatchesBallByBall.csv", 5).map(_.split(","));

  //filter records from 2015 and innings 1 and 2 . 3rd and 4th innings comes under super over
  //SEASON = 14 th column index  INNINGS=  columns index 1
  var IPLFilterRdd = IPLRdd.filter(record => record(14).toInt >= 2015 && record(1).toInt < 3);

  //calculating and filtering batsmen who has scored more than 300 runs
  //STRIKER = 4th column index
  //RUNS_IN_THAT_DELIEVRY = 7 th column index
  var batsmenRuns = IPLFilterRdd.map(record => (record(4), record(7).toDouble)).reduceByKey(_ + _).filter(_._2 > 300);

  //calculating boundaries played by batsmen
  //STRIKER = 4th column index
  //RUNS_IN_THAT_DELIEVRY = 7 th column index
  var batsmanBoundaries = IPLFilterRdd.filter(record => (record(7).toInt == 4) || (record(7).toInt == 6)).map(record => (record(4), 1.toDouble)).reduceByKey(_ + _);

  //balls played by batsmen
  //EXTRAS=8 th column index
  //STRIKER = 4th column index
  //RUNS_IN_THAT_DELIEVRY = 7 th column index
  var batsmanBalls = IPLFilterRdd.filter(record => (record(8).toInt == 0) || (record(8).toInt != 0 && record(7).toInt != 0)).map(record => (record(4), 1.toDouble)).reduceByKey(_ + _);

  var joinedData = batsmanBoundaries.join(batsmanBalls).join(batsmenRuns);
  
  //getting output as (bastman , Balls take to score baoundary) in descending order with average upto two decimals
  //r._2._1._2 = balls , r._2._1._1 = boundariesr._2._1._1
  var resultData = joinedData.coalesce(1).map(r=> (r._1,    ((((r._2._1._2/r._2._1._1)* 100).toInt).toDouble / 100   )  ) ).sortBy(r => r._2, ascending = true);

  //    resultData.foreach(println); ----> to view result
  //    resultData.saveAsTextFile("/user/output/spark/battingAVerage"); ----> to save result
}
