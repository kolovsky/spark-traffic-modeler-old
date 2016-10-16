package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 25.5.16.
 */
class Zone(idi:Int, node_idi: Int, value: Double) extends Serializable with Ordered[Zone]{
  val id = idi
  val node_id = node_idi
  val trips = value

  def compare(that: Zone): Int = {
    this.id - that.id
  }

  override def toString(): String = {
    return "("+id+", "+node_id+", "+trips+")"
  }
}
