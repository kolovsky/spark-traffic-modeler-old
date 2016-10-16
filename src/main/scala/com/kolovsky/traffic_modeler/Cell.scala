package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 19.9.16.
 */
class Cell(source: Zone, destination: Zone) extends Serializable{
  val s = source
  val d = destination
  var trips = 0.0
  var grad = 0.0
  var grad_old = 0.0
  var dir = 0.0
  var dir_old = 0.0
  var count_profile: Array[Int] = null
}
