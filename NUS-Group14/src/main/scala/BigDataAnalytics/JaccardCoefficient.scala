package BigDataAnalytics

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JLong, JSet}
import ml.sparkling.graph.api.operators.measures.EdgeMeasure
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import org.apache.spark.graphx.Graph
import scala.collection.JavaConversions._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object JaccardCoefficient extends EdgeMeasure[Double,NeighbourSet]{

  def computeValue(srcAttr:NeighbourSet,dstAttr:NeighbourSet,treatAsUndirected:Boolean=false): Double ={
    intersetSize(srcAttr, dstAttr) / unionSize(srcAttr,dstAttr)
  }


  def unionSize(neighbours1:JSet[JLong],neighbours2:JSet[JLong]):Int = {
    neighbours1.union(neighbours2).size
  }
  def intersetSize(neighbours1:JSet[JLong],neighbours2:JSet[JLong]):Int = {
    neighbours1.intersect(neighbours2).size
  }


  override def preprocess[VD:ClassTag,E:ClassTag](graph: Graph[VD, E],treatAsUndirected:Boolean=false): Graph[NeighbourSet, E] = {
    NeighboursUtils.getWithNeighbours(graph,treatAsUndirected)
  }
}
