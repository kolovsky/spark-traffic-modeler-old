package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 16.10.16.
 */
object MinOrderNodeStatic extends Ordering[(Double, Node)] {
  def compare(x:(Double, Node), y:(Double, Node)) = y._1 compare x._1
}
