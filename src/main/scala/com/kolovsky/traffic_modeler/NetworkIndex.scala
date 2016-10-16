package com.kolovsky.traffic_modeler

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.{HashMap, ListBuffer, PriorityQueue}

/**
 * Created by kolovsky on 18.5.16.
 */
class NetworkIndex extends Network with Serializable {
  val hm: HashMap[Int, Node] = HashMap.empty[Int, Node]
  var nodes: Array[Node] = null

  def addEdges(edges: Array[(Int, Int, Int, Double, Double)]): Unit = {
    if (nodes != null){
      throw new Exception("Network already contains data!")
    }
    for (e <- edges) {
      if (e._2 != e._3) {
        var source = hm.get(e._2)
        var target = hm.get(e._3)
        var s: Node = null
        var t: Node = null
        if (source == None) {
          s = new Node(e._2)
          hm += (e._2 -> s)
        }
        if (target == None) {
          t = new Node(e._3)
          hm += (e._3 -> t)
        }
      }
    }

    // add index in array for every Node
    nodes = hm.toArray.map(_._2)
    for(i <- 0 to nodes.length - 1){
      nodes(i).i = i
    }
    // create edges
    for(e <- edges){
      if (e._2 != e._3){
        val s = hm.get(e._2).get
        val t = hm.get(e._3).get
        s.edges = s.edges :+ new EdgeIndex(e._1, s.i, t.i, e._4, e._5)
      }
    }
  }

  /**
   * Search graph using Dijksta's algorithm
   * @param s - source zone
   * @param l_coef - parameter for length
   * @param t_coef - parameter for transport time
   * @return (distance_node_map, previous_edge_node map)
   */
  def dijkstraSearch(s:Zone, l_coef: Double, t_coef: Double): (Array[Double], Array[EdgeIndex]) ={
    val pq = PriorityQueue.empty[(Double, Node)](MinOrderNodeStatic)
    var s_node = idToNode(s.node_id)
    val dist: Array[Double] = Array.ofDim(nodes.length)
    val prev: Array[EdgeIndex] = Array.ofDim(nodes.length)
    for (i <- 0 to nodes.length - 1){
      dist(i) = Double.PositiveInfinity
    }
    dist(s_node.i) = 0
    pq.enqueue((0,s_node))

    var cost: Double = 0
    while (pq.nonEmpty){
      var n = pq.dequeue()._2
      for (eo <- n.edges){
        val e = eo.asInstanceOf[EdgeIndex]
        if(dist(n.i) + e.cost(l_coef, t_coef) < dist(e.t)){
          dist(e.t) = dist(n.i) + e.cost(l_coef, t_coef)
          pq.enqueue((dist(e.t), nodes(e.t)))
          prev(e.t) = e
        }
      }
    }
    return (dist, prev)
  }

  def getPaths(s: Zone, destination: Array[Zone], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double, Array[Int])] = {
    val (dist, prev) = dijkstraSearch(s, l_coef, t_coef)
    val paths: ListBuffer[(Zone, Zone, Double, Array[Int])] = ListBuffer()
    for(d <- destination){
      paths += ((s, d, dist(idToNode(d.node_id).i), getPath(prev, d.node_id)))
    }
    return paths.toArray
  }

  def getPathsTrips(s: Zone, destination: Array[(Zone, Double)], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double, Array[Int])] = {
    val (dist, prev) = dijkstraSearch(s, l_coef, t_coef)
    val paths: ListBuffer[(Zone, Zone, Double, Array[Int])] = ListBuffer()
    for(d <- destination){
      paths += ((s, d._1, d._2, getPath(prev, d._1.node_id)))
    }
    return paths.toArray
  }

  /**
   * Path form previous edge map.
   * @param prev - previous edge node map
   * @param t - target node ID
   * @return - path Array[edge_id]
   */
  private def getPath(prev: Array[EdgeIndex], t: Int): Array[Int] = {
    val t_node = hm.get(t).get
    var ae = prev(t_node.i)
    val path: ListBuffer[Int] = ListBuffer()
    while (ae != null){
      path += ae.id
      ae = prev(ae.s)
    }
    return path.reverse.toArray
  }

  def saveToFile(filename: String): Unit = {
    val output = new ObjectOutputStream(new FileOutputStream(filename))
    output.writeObject(this)
    output.close()
  }

  def loadFromFile(filename: String): Network = {
    val input = new ObjectInputStream(new FileInputStream(filename))
    return input.readObject().asInstanceOf[Network]
    //input.close()
  }

  def idToNode(id: Int): Node = {
    val n = hm.get(id)
    if(n == None){
      throw new Exception("Node with ID "+id+" does not exists.")
    } else{
      return n.get
    }
  }
  def getCosts(s: Zone, destination: Array[Zone], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double)] = {
    val (dist, prev) = dijkstraSearch(s, l_coef, t_coef)
    val paths: ListBuffer[(Zone, Zone, Double)] = ListBuffer()
    for(d <- destination){
      paths += ((s, d, dist(idToNode(d.node_id).i)))
    }
    return paths.toArray
  }
}
