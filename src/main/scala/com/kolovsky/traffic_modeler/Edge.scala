package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 18.5.16.
 */
trait Edge {
  /**
    *
    * @param l - coef for length
    * @param t - coef for time
    * @param links - Array of Links
    * @return cost
    */
  def cost(l: Double, t: Double, links: Array[Link]): Double
}
