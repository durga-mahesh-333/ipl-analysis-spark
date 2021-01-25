package strikeRate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object StrikeRate {

  val BALL = 0;
  val INNINGS = 1;
  val DELIVERY = 2;
  val BATTING_TEAM = 3;
  val STRIKER = 4;
  val NON_STRIKER = 5;
  val BOWLER = 6;
  val RUNS_IN_THAT_DELIEVRY = 7;
  val EXTRAS = 8;
  val DISMISSAL_TYPE = 9;
  val DISMISSED_PLAYER = 10;
  val TEAM_1 = 11;
  val TEAM_2 = 12;
  val DATE = 13;
  val SEASON = 14;

  def main(args: Array[String]): Unit = {
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

    //balls played by batsmen
    //EXTRAS=8 th column index
    //STRIKER = 4th column index
    //RUNS_IN_THAT_DELIEVRY = 7 th column index
    var batsmanBalls = IPLFilterRdd.filter( record=>(record(8).toInt == 0) ||(record(8).toInt != 0 && record(7).toInt != 0)).map(record=>(record(4), 1)).reduceByKey(_+_);

    var joinedData = batsmenRuns.join(batsmanBalls);

    //getting output as (bastman , strike rate) in descending order with average upto two decimals
    var resultData = joinedData.coalesce(1).map(r => (r._1, ( ( ( (r._2._1 / r._2._2)*100) * 100).toInt).toDouble / 100)).sortBy(r => r._2, ascending = false);

    //    resultData.foreach(println);
    //    resultData.saveAsTextFile("/user/output/spark/battingAVerage");
  }

}