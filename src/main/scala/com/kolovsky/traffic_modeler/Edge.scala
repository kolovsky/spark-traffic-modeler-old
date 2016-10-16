package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 18.5.16.
 */
trait Edge {
  def cost(l: Double, t: Double): Double
}
