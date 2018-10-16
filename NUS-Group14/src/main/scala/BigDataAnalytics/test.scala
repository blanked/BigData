package BigDataAnalytics

import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import org.apache.spark.SparkContext


object test {
  def main(args : Array[String]) {
    
    // importing spark context as an implicit function...
    implicit ctx:SparkContext=>new SparkContext()
    
    val directory = "/media/vboxshared/2018Sem1/BigData/"
    val fileName = "test.csv"
    val filePath = "file://" + directory + fileName
    val graph=LoadGraph.from(CSV(filePath)).load()  
    
    
  }
  
}