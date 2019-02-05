package poc

import astraea.spark.rasterframes._
import org.apache.spark.sql._

object ReadGeotiff {
  val imagePath = "/data/workspace/geotrellis-landsat-tutorial/data/r-g-nir.tif"
  
  def main(args: Array[String]): Unit = {
  
  implicit val spark = SparkSession.builder().
  master("local").appName("RasterFrames").
  config("spark.ui.enabled", "false").
  getOrCreate().
  withRasterFrames
  
  import astraea.spark.rasterframes.datasource.geotiff._
  import java.io.File
  
  val imageFile = new File(imagePath)

  val tiffRF = spark.read.geotiff.loadRF(imageFile.toURI)
  tiffRF.printSchema()


  }
}
