import geopyspark
import numpy

from pyspark.sql import SparkSession
from pyrasterframes import *

conf = geopyspark.geopyspark_conf(appName="POC")
session = SparkSession.builder.config(conf=conf).getOrCreate().withRasterFrames()
 

uri = "file:/data/workspace/geotrellis-landsat-tutorial/data/1k_tiles/"
layer_name = "landsat1K"

layer = geopyspark.query(uri=uri, layer_name=layer_name, layer_zoom=0)

rf = layer.to_rasterframe(3)
rf.show()
