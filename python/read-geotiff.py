from pyspark.sql import SparkSession
from pyrasterframes import *

session = SparkSession.builder.appName("RasterFrameDemo").config("spark.ui.enabled", "false").getOrCreate().withRasterFrames()

rf = session.read.geotiff('file:/data/workspace/geotrellis-landsat-tutorial/data/r-g-nir.tif')

rf.printSchema()

