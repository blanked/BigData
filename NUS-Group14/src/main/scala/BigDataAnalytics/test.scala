package BigDataAnalytics

import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters.{Delimiter,Quotation}
import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import org.apache.spark.SparkContext


// initialize your SparkContext as implicit value so it will be passed automatically to graph loading API


object test {
  def main(args : Array[String]) {
    val sc = newSparkContext()
    implicit ctx:SparkContext=sc
  val filePath="file:////media/vboxshared/2018Sem1/BigData/test.csv"
  val graph=LoadGraph.from(CSV(filePath)).using(Delimiter(";")).using(Quotation("'")).load()  
  }
  
}