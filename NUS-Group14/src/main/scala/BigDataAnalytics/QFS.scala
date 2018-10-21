package BigDataAnalytics

/**
 * @author ${user.name}
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx
import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId, _}
import ml.sparkling.graph.operators.measures.edge.{AdamicAdar, CommonNeighbours}
import scala.math.Ordering
import java.io._


import scala.io.Source
object QFS {

  def doQFS(graph:Graph[(Int, Int), Int], root: VertexId) : Graph[(Int, Int), Int] = {
    //Value 1 = shortest path value 2 = count. same for messag  e.
    def myVProg(vertexId: VertexId,
                value: (Int, Int),
                message: (Int, Int)): (Int, Int) = {
      if (message._1 <= value._1)
        (message._1,  message._2)
      else
        value

    }


    def mySendMsg(
                   triplet: EdgeTriplet[(Int, Int), Int])
    : Iterator[(VertexId, (Int, Int))] =
    {
      val dstNodeData = triplet.dstAttr
      val dstNodeId = triplet.dstId
      val srcNodeData = triplet.srcAttr
      val srcNodeId = triplet.srcId

      if ((dstNodeId == root) || srcNodeData._1 == Int.MaxValue )
        Iterator.empty
      else {
        Iterator((triplet.dstId, (srcNodeData._1 + 1, srcNodeData._2)))
      }
    }


    def myMergeMsg(msg1: (Int, Int), msg2: (Int, Int)): (Int, Int) = {
      if (msg1._1 < msg2._1) {
        msg1
      } else if (msg2._1 < msg1._1) {
        msg2
      } else {
        (msg1._1, msg1._2 + msg2._2)
      }
    }

    val initialGraph :Graph[(Int, Int), Int] = graph.mapVertices(
      (node, _) => if (node == root) (0, 1) else (Int.MaxValue, 0)
    )

    val qfs = initialGraph.pregel((Int.MaxValue, 0),
      maxIterations = 2)(
      myVProg,
      mySendMsg,
      myMergeMsg)
      .cache()
    qfs
  }
  
  def main(args : Array[String]) {
    println( "Hello World!" )

//    println("concat arguments = " + foo(args))

    val conf = new SparkConf().setAppName("Test QFS")
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val fs = "file://"
    val directory = "/home/j/BigData/data/"
    val fileName = "twitter_nodate.csv"
    //    val fs = "gs://"
    //    val directory = "dataproc-753f5751-93fe-4649-89ef-cb7a4c923bc1-asia-southeast1/"

    val activeFileName = "active50.csv"


    val pathLength = 2
    val minimumNumPaths = 1

    val file = spark.sparkContext.textFile(fs + directory + fileName,24)
    val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split(","))
      .map(line =>
        (line(0).toInt, line(1).toInt))

    val graph = Graph.fromEdgeTuples(edgesRDD, (Int.MaxValue, 0))

    for (line <- Source.fromFile(directory + activeFileName).getLines) {

      var rootID: VertexId = line.toInt
      var QFSgraph :Graph[(Int, Int), Int]  = doQFS(graph,  rootID)
      var mostConnection = QFSgraph.vertices.collect
      {
        case (id, vp) if vp._1 == pathLength => (id, vp)
      }
        .map(a => (a._1, a._2._2))
        .coalesce(1)
        .sortBy[Int](
        _._2
        , false
      )
        .filter(
          a => a._2 >= minimumNumPaths
        )
        .map(
          a => (rootID, a._1)
        )
      mostConnection.saveAsTextFile(fs + directory + "output_qfs_" + rootID + "/" )

    }




  }

}
