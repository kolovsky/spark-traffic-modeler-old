package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 18.5.16.
 */
class EdgeIndex(link_index: Int, source: Int, target: Int) extends Edge with Serializable {
  //val id: Int = edgeId
  val s: Int = source
  val t: Int = target
  //val length: Double = edgeCost
  //val time: Double = transport_time
  val link_i: Int = link_index

  def cost(l: Double, t: Double, links: Array[Link]): Double = {
    return l*links(link_i).l + t*links(link_i).t
  }

  override def toString: String = "(s: "+s+", t: "+t+")"
}
