import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;


object BowlingAverage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bowling Average");
    //create spark context object
    var sc = new SparkContext(conf);
    //loading a file into and spliting csv file
    var IPLRdd = sc.textFile("/user/input/IPLMatchesBallByBall.csv", 5).map(_.split(","));

    //filter records from 2015 and innings 1 and 2 . 3rd and 4th innings comes under super over
    //SEASON = 14 th column index  INNINGS=  columns index 1
    var IPLFilterRdd = IPLRdd.filter(record => record(14).toInt >= 2015 && record(1).toInt < 3);

    //filter bowlers who has bowled more than 300 balls
    //8th column = Extras bowled by bowler , if it is zero its a valid delivery 
    //6th column i= Bowler
    var bowlerBalls = IPLFilterRdd.filter( record=>(record(8).toInt == 0)).map(record=>(record(6), 1)).reduceByKey(_+_).filter(_._2 > 300);
    
    //runs given by bolwer is7th column( runs scored in that delivery) +8th column (extras Bowled)
    var bowlerRuns = IPLFilterRdd.map(record => (record(6), record(7).toDouble+record(8).toDouble)).reduceByKey(_ + _);
    
    // wickets taken is calculated by 9th record (dissmissal Type) has mentioned values
    var bowlerWickets = IPLFilterRdd.filter(record=>(List("bowled" , "caught" , "caught and bowled" , "stumped" , "lbw" ,"hit wicket"  ).contains( record(9) )   ) ).map(record=>(record(6),1.0)).reduceByKey(_+_);
    
    var wicketsRunsBallsJoin = bowlerWickets.join(bowlerRuns).join(bowlerBalls);
    
    var resultRdd= wicketsRunsBallsJoin.coalesce(1).map(r=>( (r._1 ,  ((((r._2._1._2/r._2._1._1)*100).toInt).toDouble)/100   ) ) ).sortBy(r => r._2, ascending = true);

    
    //    resultRdd.foreach(println);
    //    resultRdd.saveAsTextFile(<Path to putputFolder>);
  }
}