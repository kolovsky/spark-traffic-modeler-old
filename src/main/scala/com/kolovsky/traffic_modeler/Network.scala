package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 18.5.16.
 */
trait Network {
  /**
   * Method for build graph form list of edges
   * @param edges - Array[(edge_id, source_node_id, traget_node_id, length, transport_time)]
   */
  def addEdges(edges: Array[(Int, Int, Int, Double, Double)]): Unit

  /**
   * Return costs and paths from source zone to all destination zones. Edge cost is computed as l_coef*length + t_coef*time
   * @param s - start zone for search
   * @param destination - list of destination zone
   * @param l_coef - coeficient for length
   * @param t_coef - coeficient for time
   * @return Array[(origin, destination, cost, Array(edge id))] -- path and cost
   */
  def getPaths(s: Zone, destination: Array[Zone], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double, Array[Int])]

  /**
   * Return paths from source zone to all destination zone. Edge cost is computed as l_coef*length + t_coef*time.
   * Difference between getPaths and this method is that returns traffic for assigment.
   * @param s - start zone for search
   * @param destination - list of destination zone
   * @param l_coef - coeficient for length
   * @param t_coef - coeficient for time
   * @return Array[(origin, destination, traffic, Array(edge id))]
   */
  def getPathsTrips(s: Zone, destination: Array[(Zone, Double)], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double, Array[Int])]

  /**
   * Transform node_id to Node object
   * @param id - node ID
   * @return - Node
   */
  def idToNode(id: Int): Node
  // create graph from file
  def loadFromFile(filename: String): Network
  // save graph to file
  def saveToFile(filename: String): Unit

  /**
   * Compute all costs from source zones to all destination zones. Edge cost is computed as l_coef*length + t_coef*time.
   * @param s - start zone for search
   * @param destination - list of destination zone
   * @param l_coef - coeficient for length
   * @param t_coef - coeficient for time
   * @return Array[(source_zone, destination_zone, cost)]
   */
  def getCosts(s: Zone, destination: Array[Zone], l_coef: Double, t_coef: Double): Array[(Zone, Zone, Double)]

}
