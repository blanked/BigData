package BigDataAnalytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx

/**
 * @author ${user.name}
 */
object LinkPredictionOperator {
  
  
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    val conf = new SparkConf().setAppName("LinkPredictionOperator").setMaster("local")
//    val spark = SparkSession
//      .builder()
//      .getOrCreate()
    
  }

}