#
# test things are hooked up
# https://github.com/mGalarnyk/Installations_Mac_Ubuntu_Windows/blob/master/Spark/Estimating%20PI.ipynb
#
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages')

import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

import numpy as np

TOTAL = 1000000
dots = sc.parallelize([2.0 * np.random.random(2) - 1.0 for i in range(TOTAL)]).cache()
print("Number of random points:", dots.count())

stats = dots.stats()
print('Mean:', stats.mean())
print('stdev:', stats.stdev())
