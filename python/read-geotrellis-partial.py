import datetime
import geopyspark as gps
import numpy as np

from pyspark import SparkContext
from shapely.geometry import MultiPolygon, box

conf = gps.geopyspark_conf(master="local[*]", appName="layers")
pysc = SparkContext(conf=conf)

uri = "file:/data/workspace/geotrellis-landsat-tutorial/data/1k_tiles/"
layer_name = "landsat1K"

metadata = gps.read_layer_metadata(uri=uri,
        layer_name=layer_name,
        layer_zoom=0)
print(metadata)


# Get list of tiles
print(metadata.bounds)

# Read the first tile
tile = gps.read_value(uri=uri,
               layer_name=layer_name,
               layer_zoom=0,
               col=metadata.bounds.minKey.col,
               row=metadata.bounds.minKey.row)

print(tile)

# Read the layer
layer = gps.query(uri=uri, layer_name=layer_name, layer_zoom=0)

from pyrasterframes import *
from pyspark.sql import *


