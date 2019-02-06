import geopyspark
import numpy

from pyspark.sql import SparkSession
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *

conf = geopyspark.geopyspark_conf(appName="POC")
session = SparkSession.builder.config(conf=conf).getOrCreate().withRasterFrames()
 

uri = "file:/data/workspace/geotrellis-landsat-tutorial/data/1k_tiles/"
layer_name = "landsat1K"

layer = geopyspark.query(uri=uri, layer_name=layer_name, layer_zoom=0)

rf = layer.to_rasterframe(3)
rf.show(2)


# Show CRS
rf.tileLayerMetadata()['crs']

# Convert Tile data to array
rf.select(tileToDoubleArray("tile_1")).show(10, 80)

# Global aggregation statistics
rf.agg(aggNoDataCells("tile_1"), aggDataCells("tile_1"), aggMean("tile_1")).show(5, False)

# Tile aggregation statistics
rf.select(tileMean("tile_1"), tileMin("tile_1"), tileMax("tile_1")).show(5)
