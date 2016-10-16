package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 18.5.16.
 */
class EdgeIndex(edgeId: Int, source: Int, target: Int, edgeCost: Double, transport_time: Double) extends Edge with Serializable {
  val id: Int = edgeId
  val s: Int = source
  val t: Int = target
  val length: Double = edgeCost
  val time: Double = transport_time

  /**
   * Compute cost for edge as linear combination of length and time
   * @param l - parameter for length
   * @param t - parameter for transport time
   * @return - cost
   */
  def cost(l: Double, t: Double): Double = {
    return l*length + t*time
  }
}
