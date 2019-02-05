package poc

import java.net.URI
import astraea.spark.rasterframes._
import org.apache.spark.sql._

object ReadGeotrellis {
  val catalogUri = new URI("file:/data/workspace/geotrellis-landsat-tutorial/data/1k_tiles")
  
  def main(args: Array[String]): Unit = {
  
  implicit val spark = SparkSession.builder().
  master("local").appName("RasterFrames").
  config("spark.ui.enabled", "false").
  getOrCreate().
  withRasterFrames
  
  import astraea.spark.rasterframes.datasource.geotrellis._
  import java.io.File
  
  val catalog = spark.read.geotrellisCatalog(catalogUri)
  catalog.printSchema


  }
}
