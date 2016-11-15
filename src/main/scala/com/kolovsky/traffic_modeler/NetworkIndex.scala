package com.kolovsky.traffic_modeler

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.{HashMap, ListBuffer, PriorityQueue, Stack}

/**
 * Created by kolovsky on 18.5.16.
 */
class NetworkIndex extends Network with Serializable {
  val hm: HashMap[Int, Node] = HashMap.empty[Int, Node]
  var nodes: Array[Node] = null
  var links: Array[Link] = null
  var lastLinkIndex = 0

  def addEdges(edges: Array[(Int, Int, Int, Double, Double, Boolean)]): Unit = {
    if (nodes != null){
      throw new Exception("Network already contains data!")
    }

    val edgesFiltred = edges
    links = Array.ofDim[Link](edgesFiltred.length)

    for (e <- edgesFiltred) {
      // condition for same source and destination node
      if (e._2 != e._3) {
        hm.getOrElseUpdate(e._2, new Node(e._2))
        hm.getOrElseUpdate(e._3, new Node(e._3))
      }
    }

    // add index in array for every Node
    nodes = hm.toArray.map(_._2)
    for(i <- 0 to nodes.length - 1){
      nodes(i).i = i
    }
    // create edges
    var i = 0
    for(e <- edgesFiltred){
      if (e._2 != e._3){
        val s = hm.get(e._2).get
        val t = hm.get(e._3).get

        val e1 = new EdgeIndex(i, s.i, t.i)
        var e2: EdgeIndex = null
        s.edges = s.edges :+ e1
        s.vertexDeegre += 1
        t.vertexDeegre += 1
        //for both directions (not oneway)
        if (!e._6){
          e2 = new EdgeIndex(i, t.i, s.i)
          t.edges = t.edges :+ e2
        }
        links(i) = new Link(Array(e._1), e._4, e._5, e1, e2, i)
        lastLinkIndex = i
        i += 1
      }
    }
  }

  /**
    * This method reduce number of nodes and edges in graph.
    * Method remove nodes with deegre of vertex 2
    *  O-->O-->O-->O reduce to 0---------->O
    * @param mandatoryNodes - nodes that will not remove in reduced graph
    */
  def reduceSize(mandatoryNodes: Array[Int]): Unit ={
    // labeled mandatory nodes
    for(id <- mandatoryNodes){
      idToNode(id).vertexDeegre = -1
    }
    // start DFS
    val q = new Stack[Node]()
    q.push(nodes(0))
    // map of founded nodes
    val found = Array.ofDim[Boolean](nodes.length)
    // map of prev nodes
    val prevNode = Array.ofDim[Node](nodes.length)
    var n: Node = null
    // for merge link
    var agg = ListBuffer[Link]()
    var sourceAgg: Node = null
    var targetAgg: Node = null
    // DFS loop
    while (q.nonEmpty){
      n = q.pop()

      if(!found(n.i)){
        found(n.i) = true
        for (eo <- n.edges){
          val e = eo.asInstanceOf[EdgeIndex]
          q.push(nodes(e.t))
          prevNode(e.t) = n
          if (prevNode(n.i) != null) {
            if (n.vertexDeegre == 2 && prevNode(n.i).i != e.t) {
              if (agg.isEmpty) {
                sourceAgg = prevNode(n.i)
                targetAgg = n
                agg += links(prevNode(n.i).edges.filter(_.asInstanceOf[EdgeIndex].t == n.i).head.asInstanceOf[EdgeIndex].link_i)
              }
              agg += links(e.link_i)
              if (e.s != targetAgg.i) {
                agg.clear()
                sourceAgg = null
                targetAgg = null
              }
              targetAgg = nodes(e.t)
              if (nodes(e.t).vertexDeegre != 2 || found(e.t)) {
                if (agg.nonEmpty) {
                  mergeLink(agg.toArray, sourceAgg, targetAgg)
                  agg.clear()
                  sourceAgg = null
                  targetAgg = null
                }
              }
            }
            if (n.vertexDeegre != 2 && agg.nonEmpty) {
              agg.clear()
              sourceAgg = null
              targetAgg = null
            }
          }
        }
      }
    }
  }

  private def mergeLink(linksAgg: Array[Link], source: Node, target: Node): Unit= {
    val length = linksAgg.map(_.l).sum
    val time = linksAgg.map(_.t).sum
    val ids = linksAgg.map(_.ids).reduce(_++_)
    val isOneway = linksAgg.map(_.e2).contains(null)
    val e1 = new EdgeIndex(lastLinkIndex + 1, source.i, target.i)
    var e2: EdgeIndex = null
    if (!isOneway){
      e2 = new EdgeIndex(lastLinkIndex + 1, target.i, source.i)
      target.edges = target.edges :+ e2
    }
    source.edges = source.edges :+ e1
    if (links.length <= lastLinkIndex + 1){
      links = links ++ Array.ofDim[Link](100)
      //println("prodluzuji")
    }
    links(lastLinkIndex + 1) = new Link(ids,length, time, e1, e2, lastLinkIndex + 1)
    lastLinkIndex += 1

    //remove old edges and links
    for (l <-linksAgg){
      nodes(l.e1.s).edges = nodes(l.e1.s).edges.filter(_.asInstanceOf[EdgeIndex].link_i != l.i)
      if(l.e2 != null){
        nodes(l.e2.s).edges = nodes(l.e2.s).edges.filter(_.asInstanceOf[EdgeIndex].link_i != l.i)
      }
      links(l.i) = null
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
    var pp = 0 //smazat
    var cost: Double = 0
    while (pq.nonEmpty){
      var n = pq.dequeue()._2
      for (eo <- n.edges){
        pp += 1 //spazat
        val e = eo.asInstanceOf[EdgeIndex]
        if(dist(n.i) + e.cost(l_coef, t_coef, links) < dist(e.t)){
          dist(e.t) = dist(n.i) + e.cost(l_coef, t_coef, links)
          pq.enqueue((dist(e.t), nodes(e.t)))
          prev(e.t) = e
        }
      }
    }
    //println("Dij relax: "+pp)
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
      path ++= links(ae.link_i).ids
      ae = prev(ae.s)
    }
    return path.reverse.toArray
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
