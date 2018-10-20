/**
  * @author Hui Jun
  *         Description: Sample file for sparkling graph link prediction
  */

package BigDataAnalytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx
import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.measures.edge.{AdamicAdar, CommonNeighbours}

import ml.sparkling.graph._

import ml.sparkling.graph.operators.OperatorsDSL

import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.graphx._
import ml.sparkling.graph.operators.algorithms.link.BasicLinkPredictor
import ml.sparkling.graph.api.operators.measures.{EdgeMeasure, VertexMeasureConfiguration}
//import ml.sparkling.graph.api.operators.measures._

// Added for partitioning
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN


object LinkPredictionOperator {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinkPredictionOperator").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val filePath = args(0)
    val outPath = args(1)
    val threshold = args(2).toInt
/*    val fs = "file:///"
    val directory = "Users/liyan/Downloads/first/"
    val fileName = "first.csv" */
/*    val fs = "gs://"
  //  val directory = "dataproc-753f5751-93fe-4649-89ef-cb7a4c923bc1-asia-southeast1/"
  //  val fileName = "flickr-full.csv"*/

  //  val filePath = fs + directory + fileName
    //    implicit sc:SparkContext => spark.sparkContext
    implicit val sc = spark.sparkContext
    val graph: Graph[String, String] = LoadGraph.from(CSV(filePath)).load()

    val components: Graph[ComponentID, String] = PSCAN.computeConnectedComponents(graph)
    components.saveAsTextFile(outPath)

    val predictedEdges = BasicLinkPredictor.predictLinks(graph, CommonNeighbours, threshold, false)
    //    val predictedEdges: RDD[(VertexId, VertexId)] = graph.predictLinks(edgeMeasure=CommonNeighbours,threshold=10, treatAsUndirected=false)

    //println("Size of RDD: " + predictedEdges.count())
    predictedEdges.saveAsTextFile(outPath)


    println("Complete!")

  }

}
