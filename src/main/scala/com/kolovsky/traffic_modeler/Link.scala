package com.kolovsky.traffic_modeler

/**
  * Created by kolovsky on 23.10.16.
  */
class Link (edges_id: Array[Int], length: Double, time: Double, e1i: EdgeIndex, e2i: EdgeIndex,index: Int) {
  val ids = edges_id
  val l = length
  val t = time
  val e1 = e1i
  val e2 = e2i
  val i = index

  override def toString(): String = {
    return ids(0)+ ""
  }
}
