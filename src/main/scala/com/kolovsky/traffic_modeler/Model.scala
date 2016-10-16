package com.kolovsky.traffic_modeler

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * Created by kolovsky on 19.5.16.
 */
class Model(g: Network, zone: Array[Zone], confi: ModelConf) extends Serializable{
  val conf = confi
  val network = g
  //(node_id, Pi)
  val zones = zone

  /**
   * Compute cost matrix.
   * @param RDD_zones input RDD of zones
   * @return - cost matrix RDD[(source, destination, cost)]
   */
  def costMatrix(RDD_zones: RDD[Zone]): RDD[(Zone, Zone, Double)] = {
    val RDD_cost = RDD_zones
      .flatMap(z => network.getCosts(z, zones, conf.length_coef, conf.time_coef))
      .filter(p => p._3 != 0 && !p._3.isInfinity)
    return RDD_cost
  }

  /**
   *
   * Compute OD Matrix from cost matrix and vector of zones.
   * The method apply distibution function (Tij = F(dij) * Ti * Tj) and balance the matrix.
   * @param costMartixP - input cost matric RDD[(source, destination, cost)]
   * @return - OD Matrix (is not calibrate) RDD[(source, destination, trips)]
   */
  def estimateODMatrix(costMartixP: RDD[(Zone, Zone, Double)]): RDD[(Zone, Zone, Double)] = {
    var init = costMartixP.map(model)
    init.persist()

    val maxIter = 1
    var coef: Double = 0
    var i = 0
    while (math.abs(coef - 1) > 0.01 && i < maxIter){
      val sum_coef_row= init.map(c => (c._1.id,(c._1,c._3)))
        .reduceByKey((a,b) => (a._1,a._2 + b._2)).map(x => (x._1,x._2._1.trips / x._2._2))

      val after_row = init.map(c => (c._1.id, c))
        .join(sum_coef_row)
        .map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))//

      val sum_coef_column = after_row.map(c => (c._2.id,(c._2,c._3)))
        .reduceByKey((a,b) => (a._1,a._2 + b._2)).map(x => (x._1,x._2._1.trips / x._2._2))
      sum_coef_column.persist()

      val after_column = after_row.map(c => (c._2.id, c))
        .join(sum_coef_column)
        .map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))
      val init_old = init
      init = after_column.persist()
      init_old.unpersist(false)

      coef = sum_coef_column.map(_._2).reduce(_+_) / sum_coef_column.count()
      sum_coef_column.unpersist(false)
      println("Balance coef: "+coef)
      i += 1
    }
    return init
  }
  /**
   * Assigmnet trips to count profile.
   * @param cells ODM (RDD[com.kolovsky.traffic_modeler.Cell])
   * @return RDD of count profile with traffic RDD[(edge_id, trips)]
   */
  def assigmentPairsToEdge3(cells: RDD[Cell]): RDD[(Int, Double)] = {
    val pairs = cells.flatMap(c => c.count_profile.map(id => (id, c.trips)))
      .reduceByKey(_+_)
    return pairs
  }

  /**
   * Path contains only edge_id with reference count traffic.
   * @param in_OD input OD Matrix
   * @param v_hm HashMap of reference traffic (edge_id, traffic)
   * @return RDD[(source, destination, trips, Array[edge_id])]
   */
  def filtredPath(in_OD: RDD[(Zone, Zone, Double)], v_hm: HashMap[Int, Double]): RDD[(Zone, Zone, Double, Array[Int])] = {
    def filter(p: Array[Int]): Array[Int] ={
      val out = ListBuffer[Int]()
      for (l <- p){
        if (v_hm.get(l) != None){
          out += l
        }
      }
      return out.toArray
    }
    val pairs = in_OD.map(c => (c._1.id,(c._1,Array((c._2, c._3)))))
      .reduceByKey((a,b) => (a._1, b._2 ++ a._2))
      .map(x => x._2 )
      .flatMap(r => network.getPathsTrips(r._1,r._2.toArray, conf.length_coef, conf.time_coef))
      .map(x => (x._1, x._2, x._3, filter(x._4)))

    return pairs
  }

  /**
   * Compute value of Z function (for minimalization).
   * @param v_hm - HashMap[(edge_id, trips)] of calibrate data (reference counts)
   * @param v_m - model traffic on counts profile
   * @return - value of Z function
   */
  def Z(v_hm: HashMap[Int, Double], v_m: RDD[(Int, Double)]): Double = {
    return v_m.map(mod => math.pow((v_hm.get(mod._1).get - mod._2),2) ).reduce(_+_)
  }

  /**
   * Compute Z derivation by gi (one cell in OD Matrix).
   * @param path - path between two com.kolovsky.traffic_modeler.Zone belonging one cell
   * @param v_hm - HashMap[edge_id, count] with reference counts data
   * @param v_m_hm - HashMap[edge_id, model_traffic] model traffic on counts profile
   * @return derivation (Double)
   */
  def gradZi(path: Array[Int],v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double]): Double = {
    var out: Double = 0
    for (a <- path){
      val v_n = v_hm.get(a)
      val v_m_n = v_m_hm.get(a)
      if (v_n != None && v_m_n != None){
        out += (v_m_n.get - v_n.get)
      }
    }
    return out
  }

  /**
   * Get lambda definition interval from direction
   * condition g_i > 0
   * @param dir
   * @return bound (low, up)
   */
  def getBoundFromDir(dir: Double): (Double, Double) ={
    //bound
    var upB = Double.PositiveInfinity
    var downB = Double.NegativeInfinity
    if (dir > 0){
      upB = 1 / dir
    }
    if (dir < 0){
      downB = 1 / dir
    }
    return (downB, upB)
  }

  /**
   * Perform one dimensional minimalization in current direction
   * @param v_hm - reference traffic count HashMap[(edge_id, count)]
   * @param v_m_hm - model traffic HashMap[(edge_id, count)]
   * @param cells - RDD of com.kolovsky.traffic_modeler.Cell
   * @return RDD of com.kolovsky.traffic_modeler.Cell
   */
  def oneDimensionalMinimalization(v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double], cells: RDD[Cell]): RDD[Cell] ={
    // (zone, zone, trips, direction, path)
    val bound = cells.map(c => getBoundFromDir(c.dir)).reduce((a,b) => (math.max(a._1,b._1), math.min(a._2,b._2)))

    // derivative Va under lambda
    val vDa = cells.flatMap(c => c.count_profile.map(id => (id, - c.dir * c.trips))).reduceByKey(_+_)
    vDa.persist(StorageLevel.MEMORY_AND_DISK)

    val citatel = vDa.map(v => {
      v._2 * (v_hm.get(v._1).get - v_m_hm.get(v._1).get)
    }).reduce(_+_)

    val jmenovatel = vDa.map(x => math.pow(x._2, 2)).reduce(_+_)

    var lambdaP =  citatel / jmenovatel
    var l: Double = 0

    // kontrola jestly je lambda ve spravnem intervalu (lambda * grad <= 1)
    println("Lambda interval is (condition lambda * grad <= 1)" + bound.toString())

    if (lambdaP >= bound._1 && lambdaP <= bound._2){
      l = lambdaP
    }
    else if (lambdaP < bound._1){
      l = bound._1
    }
    else if (lambdaP > bound._2){
      l = bound._2
    }
    println("Lambda is " + l)
    vDa.unpersist(false)
    return cells.map(c => {
      c.trips = c.trips * (1 - l * c.dir)
      c
    })
  }
  /**
   * This method find minimum of Z function using Gradient method.
   * method value:
   *  "GM" - gradient method with long steps (Spiess 92)
   *  "PR" - Conjugent gradient method (Polak-Ribiereova metod)
   *  "FR" - Conjugent gradient method (Fletcher-Reevesova metod)
   * @param in_OD - Input OD Matrix RDD[(com.kolovsky.traffic_modeler.Zone, com.kolovsky.traffic_modeler.Zone, trips)]
   * @param v - vector with calibrate data Array[(edge_id, count)]
   * @param maxNumIter - maximum number of iteration.
   * @return calibrate OD Matrix
   */
  def calibrateODMatrix(in_OD: RDD[(Zone, Zone, Double)], v: Array[(Int, Double)], maxNumIter: Int, method: String): RDD[(Zone, Zone, Double)] = {
    val v_hm = new HashMap[Int, Double]()
    v_hm ++= v

    val p = filtredPath(in_OD, v_hm)
    var odm = p.map(x => {
      val c = new Cell(x._1, x._2)
      c.count_profile = x._4
      c.trips = x._3
      c
    })
    odm.persist(StorageLevel.MEMORY_AND_DISK)
    odm.localCheckpoint()

    var odm_uc: RDD[Cell] = null
    var odm_dir: RDD[Cell] = null



    var v_m = assigmentPairsToEdge3(odm)
    v_m.persist(StorageLevel.MEMORY_AND_DISK)

    println("Value of sqrt(Z(g)/n) function ("+0+") is " + math.sqrt(Z(v_hm, v_m)/v.length))

    for (i <- 1 to maxNumIter){

      val v_m_hm = new HashMap[Int, Double]()
      v_m_hm ++= v_m.collect()

      //gradient
      odm_uc = odm
      val odm_grad = odm.map(c => {
        c.grad_old = c.grad
        c.grad = gradZi(c.count_profile, v_hm, v_m_hm)
        c
      })

      if (i == 1 || method == "GM"){
        odm_dir = odm_grad.map(c =>{
          c.dir = c.grad
          c
        })
      }
      else{
        odm_grad.persist(StorageLevel.MEMORY_AND_DISK)
        var beta = 0.0
        if (method == "PR"){
          beta = odm_grad.map(c => (c.grad - c.grad_old) * c.grad).reduce(_+_) / odm_grad.map(c => math.pow(c.grad_old, 2)).reduce(_+_)
        }
        else if(method == "FR") {
          beta = odm_grad.map(c => math.pow(c.grad, 2)).reduce(_+_) / odm_grad.map(c => math.pow(c.grad_old, 2)).reduce(_+_)
        }

        odm_dir = odm.map(c =>{
          c.dir_old = c.dir
          c.dir = c.grad + beta * c.dir_old
          c
        })

      }
      odm_dir.persist(StorageLevel.MEMORY_AND_DISK)
      odm_uc = odm
      odm = oneDimensionalMinimalization(v_hm, v_m_hm, odm_dir)

      //v_m.unpersist(false)

      odm.persist(StorageLevel.MEMORY_AND_DISK)
      odm.localCheckpoint()



      v_m = assigmentPairsToEdge3(odm)
      v_m.persist(StorageLevel.MEMORY_AND_DISK)
      println("Value of sqrt(Z(g)/n) function ("+i+") is " + math.sqrt(Z(v_hm, v_m)/v.length))

      //unpersist section
      odm_grad.unpersist(false)
      odm_dir.unpersist(false)
      odm_uc.unpersist(false)
    }
    return odm.map(c => (c.s, c.d, c.trips))
  }

  /**
   * Assigment traffic to network. Method compute all paths between zones and join this paths to the OD Matrix.
   * @deprecated
   * @param ODMatrix - input OD matrix
   * @param RDD_zones - RDD of zones
   * @return - RDD[(edge_id, traffic)]
   */
  def assigmentTraffic(ODMatrix: RDD[(Zone, Zone, Double)], RDD_zones: RDD[Zone]): RDD[(Int, Double)] = {
    val paths = RDD_zones
      .flatMap(z => network.getPaths(z, zones, conf.length_coef, conf.time_coef))
      .filter(p => p._3 != 0 && !p._3.isInfinity)
    val join = ODMatrix.map(c => ((c._1.id, c._2.id),c._3)).join(paths.map(p => ((p._1.id, p._2.id),p._4)))
    val out = join.flatMap(x => x._2._2.map(y => (y, x._2._1))).reduceByKey(_+_, 100)
    return out
  }

  /**
   * Assigment traffic to network. Better method (faster) then "assigmentTraffic" method.
   * @param ODMatrix - input OD Matrix
   * @return RDD[(edge_id, traffic)]
   */
  def assigmentTraffic2(ODMatrix: RDD[(Zone, Zone, Double)]): RDD[(Int, Double)] = {
    val out = ODMatrix.map(c => (c._1.id,(c._1,ListBuffer((c._2, c._3)))))
      .reduceByKey((a,b) => (a._1, b._2 ++ a._2))
      .map(x => x._2 )
      .flatMap(r => network.getPathsTrips(r._1,r._2.toArray, conf.length_coef, conf.time_coef))
      .flatMap(x => x._4.map(id => (id, x._3))).reduceByKey(_+_)
    return out
  }
  private def model(cell: (Zone, Zone, Double)): (Zone, Zone, Double) = {
    val o = cell._1
    val d = cell._2
    val cost = cell._3
    val trips = o.trips * d.trips * conf.F(cost)
    return (o, d, trips)
  }
}
