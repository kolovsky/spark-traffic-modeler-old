package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 17.2.16.
 */
class Node(ida: Int) extends Serializable{
  var id: Int = ida
  var i: Int = -1
  var edges: List[Edge] = List()
}
