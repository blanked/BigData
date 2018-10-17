/**
 * @author Hui Jun
 * Description: Sample file for sparkling graph link prediction
 */

package BigDataAnalytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
import ml.sparkling.graph.api.operators.measures.{EdgeMeasure, VertexMeasureConfiguration}
//import ml.sparkling.graph.api.operators.measures._

// Added for partitioning
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN



object LinkPredictionOperator {
  
  
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    val conf = new SparkConf().setAppName("LinkPredictionOperator").setMaster("local")
    val spark = SparkSession
      .builder()
      .getOrCreate()
    
    
    val directory = "/media/vboxshared/2018Sem1/BigData/data/"
//    val fileName = "flickr-full.csv"
//    val fileName = "flickr-nodate.csv"
    val fileName = "small_nodate.csv"
    
    val filePath = "file://" + directory + fileName
    implicit sc:SparkContext => spark.sparkContext
    val graph:Graph[Int, Int] = LoadGraph.from(CSV(filePath)).load()
    
//    val predictedEdges = BasicLinkPredictor.predictLinks(graph, CommonNeighbours, 10, false)
    val predictedEdges: RDD[(VertexId, VertexId)] = graph.predictLinks(edgeMeasure=CommonNeighbours,threshold=2, treatAsUndirected=false)
//    predictedEdges.saveAsTextFile("file://" + directory + "output")
    
    // Below is working..its only edgemeasure that is giving the exception
//    val centralityGraph: Graph[Double, _] = graph.closenessCentrality()
    
//    val communityDetectionMethod=PSCAN
//    val partitionedGraph1 =  CommunityBasedPartitioning.partitionGraphBy(graph,communityDetectionMethod)
    
    
    
    
    println("Complete!")
    
  }

}