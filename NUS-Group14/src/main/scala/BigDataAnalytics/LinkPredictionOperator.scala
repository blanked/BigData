/**
  * @author Hui Jun
  *         Description: Sample file for sparkling graph link prediction
  */

package BigDataAnalytics


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx
import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV

import ml.sparkling.graph.operators.measures.edge.{AdamicAdar, CommonNeighbours}

import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.graphx._
import ml.sparkling.graph.operators.algorithms.link.BasicLinkPredictor


// Added for partitioning
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN

// Added loading parameters
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters.{Delimiter, NoHeader, Partitions, Quotation}


object LinkPredictionOperator {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinkPredictionOperator").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    implicit val sc = spark.sparkContext

    if(args.length >= 4) {
      val algo = args(0)
      val filePath = args(1)
      val outPath = args(2)
      val threshold = args(3).toInt

      val graph: Graph[String, String] = LoadGraph.from(CSV(filePath)).load()
      var predictedEdges: RDD[(graphx.VertexId, graphx.VertexId)] = null
      algo match {
        case "JC" => predictedEdges = BasicLinkPredictor.predictLinks(graph, JaccardCoefficient, threshold, false)
        case "CN" => predictedEdges = BasicLinkPredictor.predictLinks(graph, CommonNeighbours, threshold, false)
        case _ => println("Undefine predictor")
          throw new IllegalArgumentException("Undefine predictor. \n CN -> Common Neighbors \n JC -> Jaccard Coefficient ")
      }
      predictedEdges.saveAsTextFile(outPath)
      println("Complete!")
    } else {
      println("args: [predictor] [input] [output] [threshold]")
    }

  }

}
