package com.kolovsky.traffic_modeler

import org.apache.spark.rdd.RDD

/**
  * Created by kolovsky on 10.12.16.
  */
trait ODMatrixEstimator {
  /**
    *
    * @return OD Matrix
    */
  def estimate(): RDD[(Zone, Zone, Double, Array[Int])]

  /**
    *
    * @return log string (e.g acuracy)
    */
  def getLog(): String
}
