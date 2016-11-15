package com.kolovsky.traffic_modeler

/**
  * Created by kolovsky on 9.11.16.
  */
class ODPair(source: Zone, destination: Zone) extends Serializable{
  val s: Zone = source
  val t: Zone = destination
  var trips: Double = 0
  var grad: Double = 0
  var dir: Double = 0
  var path: Array[Int] = null
  var sum_by_row = 0.0
  var sum_by_column = 0.0
  var lij = 0.0
  var uij = 0.0
  def setTrips(t: Double): ODPair ={
    trips = t
    return this
  }
  def setGrad(g: Double): ODPair ={
    grad = g
    return this
  }
  def setDir(d: Double): ODPair ={
    dir = d
    return this
  }
  def setPath(p: Array[Int]): ODPair ={
    path = p
    return this
  }
  def copy(): ODPair ={
    val o = new ODPair(s, t)
    o.trips = trips
    o.grad = grad
    o.dir = dir
    val a = Array.ofDim[Int](path.length)
    path.copyToArray(a)
    o.path = a
    o.sum_by_column = sum_by_column
    o.sum_by_row = sum_by_row
    o.lij = lij
    o.uij = uij
    o
  }
  def check(): Boolean ={
    if (lij <= trips && trips <= uij ){
      return true
    }
    return false
  }
  def toCell(): Cell ={
    val c = new Cell(s, t)
    c.count_profile = path
    c.dir = dir
    c.grad = grad
    c.trips = trips
    return c
  }
}
