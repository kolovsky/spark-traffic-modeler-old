package com.kolovsky.traffic_modeler

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * Created by kolovsky on 19.5.16.
 */
class Model(g: Network, zone: Array[Zone], confi: ModelConf) extends Serializable{
  val conf = confi
  val network = g
  //(node_id, Pi)
  val zones = zone
  //println("HHH")

  /**
   * Compute cost matrix.
   * @param RDD_zones input RDD of zones
   * @return - cost matrix RDD[(source, destination, cost)]
   */
  def costMatrix(RDD_zones: RDD[Zone]): RDD[(Zone, Zone, Double)] = {
    val RDD_cost = RDD_zones
      .flatMap(z => network.getCosts(z, zones, conf.length_coef, conf.time_coef, conf.searchRadius))
      .filter(p => p._3 != 0 && !p._3.isInfinity)
    return RDD_cost
  }

  /**
   *
   * Compute OD Matrix from cost matrix and vector of zones.
   * The method apply distibution function (Tij = F(dij) * Ti * Tj) and balance the matrix.
   * @deprecated use function estimateODMatrixK
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
  private def assigmentPairsToEdge3(cells: RDD[Cell]): RDD[(Int, Double)] = {
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
  private def filtredPath(in_OD: RDD[(Zone, Zone, Double)], v_hm: HashMap[Int, Double]): RDD[(Zone, Zone, Double, Array[Int])] = {
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
      .flatMap(r => {
        val ret = network.getPathsTrips(r._1,r._2, conf.length_coef, conf.time_coef, conf.searchRadius)
        ret
      })
      .map(x => {
        (x._1, x._2, x._3, filter(x._4))
      })

    return pairs
  }

  /**
   * Compute value of Z function (for minimalization).
   * @param v_hm - HashMap[(edge_id, trips)] of calibrate data (reference counts)
   * @param v_m - model traffic on counts profile
   * @return - value of Z function
   */
  def Z(v_hm: HashMap[Int, Double], v_m: RDD[(Int, Double)]): Double = {
    return v_m.map(mod => math.pow(v_hm(mod._1) - mod._2, 2)).reduce(_+_)
  }

  /**
   * Compute Z derivation by gi (one cell in OD Matrix).
   * @param path - path between two com.kolovsky.traffic_modeler.Zone belonging one cell
   * @param v_hm - HashMap[edge_id, count] with reference counts data
   * @param v_m_hm - HashMap[edge_id, model_traffic] model traffic on counts profile
   * @return derivation (Double)
   */
  private def gradZi(path: Array[Int],v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double]): Double = {
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
  private def getBoundFromDir(dir: Double): (Double, Double) ={
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
  private def oneDimensionalMinimalization(v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double], cells: RDD[Cell]): RDD[Cell] ={
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
      .flatMap(z => network.getPaths(z, zones, conf.length_coef, conf.time_coef, conf.searchRadius))
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
      .flatMap(r => network.getPathsTrips(r._1,r._2.toArray, conf.length_coef, conf.time_coef, conf.searchRadius))
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

  /**
    * Calibrate OD matrix using algorithm publicated in Doblas 2005
    * EXPERIMENTAL
    *
    * Doblas, J. & Benitez, F. G. An approach to estimating and updating origin--destination matrices based upon traffic counts
    * preserving the prior structure of a survey matrix
    * Transportation Research Part B: Methodological, Elsevier, 2005, 39, 565-591
    *
    * @param in_OD - input OD Matrix RDD[(origin, destination, trips)]
    * @param v - vector with counts profile Array[(edge_id, couts)]
    * @param m - max number of iteration for minimaliation of subproblem (for Frank-Wolfe algorithm)
    * @param s - max number of subproblem
    * @param S - scale factor (mode in Doblas 2005)
    * @param limit - function where return limits. f(trips, mode, level)
    *   mode: 'up' for upper bound and 'lo' for lower bound
    *   level:  0 for OD Pair
    *           1 for row/column,
    *           2 for all trips in network
    * @return OD Matrix
    */
  def calibrateODMatrixLagrange(in_OD: RDD[(Zone, Zone, Double)], v: Array[(Int, Double)], m: Int, s: Int, S: Double, limit: (Double, String, Int) => Double): RDD[(Zone, Zone, Double)] = {
    val v_hm = new HashMap[Int, Double]()
    v_hm ++= v
    // compute path and filter to count profile
    val paths_trips = filtredPath(in_OD, v_hm)
    // init ODM
    var init_odm = paths_trips.map(c => {
      var odp = new ODPair(c._1, c._2)
      odp.setPath(c._4).setTrips(c._3)
      odp.lij = limit(odp.trips, "lo", 0)
      odp.uij = limit(odp.trips, "up", 0)
      odp
    })
    init_odm.persist()
    init_odm.localCheckpoint()

    // compute model traffic on count profile (assigment)
    var model_traffic = init_odm.flatMap(x => x.path.map(id => (id, x.trips))).reduceByKey(_+_)
    model_traffic.persist()

    // create HashMap from model traffic
    val v_m_hm = new HashMap[Int, Double]()
    v_m_hm ++= model_traffic.collect()

    // compure value of Z function
    var Z = model_traffic.map(t => math.pow(t._2 - v_hm.get(t._1).get, 2)).reduce(_+_)
    println("Z is " + math.sqrt(Z/v.length))

    //initializate lagrange coeficient and limits
    var sum_by_row = init_odm.map(c => (c.s, c.trips)).reduceByKey(_+_).collect()
    var sum_by_column = init_odm.map(c => (c.t, c.trips)).reduceByKey(_+_).collect()
    var all_trips = init_odm.map(_.trips).reduce(_+_)
    val lag = new Lagrange(sum_by_row, sum_by_column, all_trips, limit)

    var odm_dir: RDD[ODPair] = null
    var odm_grad: RDD[ODPair] = null
    var out_odm: RDD[ODPair] = null

    for (i <- 0 to s) {
      println("s = "+i)
      var init_odm_old:RDD[ODPair] = null
      for (j <- 0 to m) {
        // compute gradient of lagrange function
        odm_grad = gradLagrange(init_odm, v_hm, v_m_hm, lag, S)
        // estimate search direction for Frank-Wolfe algorithm
        odm_dir = searchDirection(odm_grad)

        odm_dir.persist()
        odm_dir.localCheckpoint()
        // optimal lambda for direction (line search)
        val lam = lambda(odm_dir, v_hm, v_m_hm, lag, S)
        println("Lambda:"+lam)

        // new ODM
        out_odm = odm_dir.map(c => c.setTrips(c.trips + lam * c.dir)).map(_.copy())
        out_odm.persist()
        out_odm.localCheckpoint()

        // compute model traffic and evaluate objective function
        model_traffic = out_odm.flatMap(x => x.path.map(id => (id, x.trips))).reduceByKey(_ + _)
        v_m_hm.clear()
        v_m_hm ++= model_traffic.collect()
        Z = model_traffic.map(t => math.pow(t._2 - v_hm.get(t._1).get, 2)).reduce(_ + _)
        println("Z is " + math.sqrt(Z/v.length))

        init_odm_old = init_odm
        init_odm = out_odm
        init_odm_old.unpersist()
        odm_dir.unpersist()
      }
      // update Lagrage coeficients
      sum_by_row = init_odm.map(c => (c.s, c.trips)).reduceByKey(_+_).collect()
      sum_by_column = init_odm.map(c => (c.t, c.trips)).reduceByKey(_+_).collect()
      all_trips = init_odm.map(_.trips).reduce(_+_)
      lag.updateLagrange(sum_by_row, sum_by_column, all_trips)
    }
    // check contraints
    //for Cell
    println("Check:"+init_odm.filter(_.check()).count()/init_odm.count().toDouble)
    // for Row
    val dd = init_odm.map(c => (c.s, c.trips)).reduceByKey(_+_).filter(x => {
      var ret = false
      if (lag.getLRow(x._1) < x._2 && lag.getURow(x._1) > x._2){
        ret = true
      }
      ret
    }).count()
    println("Check row: "+ (dd/sum_by_row.length.toDouble))
    // for all trips in network
    if(lag.getL() <= all_trips && lag.getU() >= all_trips){
      println("Check all trips: OK!")
    }
    else {
      println("Check all trips: NOT OK!")
    }

    return init_odm.map(c => (c.s, c.t, c.trips))
  }

  /**
    * Compute gradient of Lagransian function
    * EXPERIMENTAL
    * @param odm OD Matrix RDD[ODPair]
    * @param v_hm - reference counts
    * @param v_m_hm - model traffic
    * @param lag - lagrange coeficients, Lagrange
    * @param S - scale factor (more in Doblas 2005)
    * @return RDD[ODPair] with grad
    */
  private def gradLagrange(odm: RDD[ODPair], v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double], lag: Lagrange, S: Double): RDD[ODPair] = {
    // derivate P respekt Tij_traffic-counts (28)
    val der_Z = odm.map(c => {
      val der = 2 * c.path.map(id => v_m_hm.get(id).get - v_hm.get(id).get).sum
      //println(der)
      c.setGrad(der)
    })
    // derivate P respect Tij_restriction (29)
    val all_trips = odm.map(_.trips).reduce(_+_)
    val sum_by_j = odm.map(c => (c.s, c.trips)).reduceByKey(_+_)
    val sum_by_i = odm.map(c => (c.t, c.trips)).reduceByKey(_+_)
    val odm_sum = der_Z.map(c => (c.s, c)).join(sum_by_j).map(x => {
      val c = x._2._1
      c.sum_by_row = x._2._2
      c
    }).map(c => (c.t, c)).join(sum_by_i).map(x =>{
      val c = x._2._1
      c.sum_by_column = x._2._2
      c
    })
    val odm_grad = odm_sum.map(c =>{
      val i = c.s
      val j = c.t
      val values = 2*S*( BO(c.sum_by_row - lag.getLRow(i) + lag.getBetaRow(i)) - BO(lag.getURow(i) - c.sum_by_row + lag.getDeltaRow(i)) )
      + 2*S*( BO(c.sum_by_column - lag.getLColumn(j) + lag.getBetaColumn(j)) - BO(lag.getUColumn(j) - c.sum_by_column + lag.getDeltaColumn(j)) )
      + 2*S*( BO(all_trips - lag.getL() + lag.getBeta()) - BO(lag.getU() - all_trips + lag.getDelta()) )
      c.grad += values
      c
    })
    return odm_grad
  }

  private def searchDirection(odm: RDD[ODPair]): RDD[ODPair] = {
    odm.map(c => {

      if (c.grad > 0){
        c.dir = c.lij - c.trips
      }
      if (c.grad == 0){
        c.dir = c.trips - c.trips
      }
      if (c.grad < 0){
        c.dir = c.uij - c.trips
      }
      //println(c.dir)
      c
    })
  }

  private def lambda(odm: RDD[ODPair],v_hm: HashMap[Int, Double], v_m_hm: HashMap[Int, Double], lag: Lagrange, S: Double): Double = {
    val v_der = odm.flatMap(c => c.path.map(id => (id, c.dir))).reduceByKey(_+_)

    def P(lambda: Double): Double ={
      val r1 = 2 * v_der.map(v_c => (v_m_hm.get(v_c._1).get + lambda*v_c._2 - v_hm.get(v_c._1).get)*v_c._2 ).reduce(_+_)
      //println(r1)
      val r2 = - 2 * S * odm.map(c => (c.s, (c.trips, c.dir)))
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
        .map(x => BO(lag.getURow(x._1) - x._2._1 - lambda * x._2._2 + lag.getDeltaRow(x._1))*x._2._2 ).reduce(_+_)
      val r3 = 2 * S * odm.map(c => (c.s, (c.trips, c.dir)))
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
        .map(x => BO(x._2._1 + lambda * x._2._2 - lag.getLRow(x._1) + lag.getBetaRow(x._1))*x._2._2 ).reduce(_+_)
      val r4 = - 2 * S * odm.map(c => (c.t, (c.trips, c.dir)))
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
        .map(x => BO(lag.getUColumn(x._1) - x._2._1 - lambda * x._2._2 + lag.getDeltaColumn(x._1))*x._2._2 ).reduce(_+_)
      val r5 = 2 * S * odm.map(c => (c.t, (c.trips, c.dir)))
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
        .map(x => BO(x._2._1 + lambda * x._2._2 - lag.getLColumn(x._1) + lag.getBetaColumn(x._1))*x._2._2 ).reduce(_+_)
      val all_trips = odm.map(_.trips).reduce(_+_)
      val all_dir = odm.map(_.dir).reduce(_+_)
      val r6 = -2*S*BO(lag.getU()-all_trips - lambda * all_dir + lag.getDelta())* all_dir
      val r7 = 2*S*BO(all_trips + lambda * all_dir - lag.getL() + lag.getBeta())* all_dir
      return r1 + r2 + r3 + r4 + r5 + r6 + r7
    }
    // finging zero point
    val F0 = P(0)
    val F1 = P(1)
    //for (ii <- 0 to 10){
    //  println(P(ii/10.0))
    //}

    var lam = 0.5
    val F0a = math.abs(F0)
    val F1a = math.abs(F1)

    if (F0 * F1 < 0){
      lam = F0a / (F0a + F1a)
    }
    else{
      if(F0a < F1a){
        lam = 0
      }
      else{
        lam = 1
      }
    }
    return lam
  }

  /**
    * Bracket operator (more in Raclaites 2007)
    * @param x
    * @return
    */
  private def BO(x: Double): Double = {
    if (x <= 0){
      return x
    }
    return 0
  }

  /**
    * Estimate OD Matrix using gravity model. Deterrence function is f(c) = c^(alpha) * exp(- beta * c)
    * Tij = k * Oi * Dj * Ai * Bj * f(Cij)
    * Oi is number of trips starts in zone i
    * Dj is number of trips ends in zone j
    * Ai and Bj are balance coeficients
    * k is determinated using least squares method (min (Va - Va_ref)^2)
    * @param RDD_zones zones RDD[Zone]
    * @param init init deterrence function parameter (alpha, beta, k)
    * @param v reference traffic count Array[(edge_id, counts)]
    * @return OD Matrix RDD[(origin, destination, trips)]
    */
  def estimateODMatrixK(RDD_zones: RDD[Zone], init: (Double, Double, Double), v: Array[(Int, Double)]): RDD[(Zone, Zone, Double)] = {
    // hash map of counts
    val v_hm = new HashMap[Int, Double]()
    v_hm ++= v
    // paths + cost
    val cost_filtred_path = RDD_zones
      .flatMap(z => network.getPaths(z, zones, conf.length_coef, conf.time_coef, conf.searchRadius))
      .filter(p => p._3 != 0 && !p._3.isInfinity)
      .map(x =>{
        val p = x._4
        val out = ListBuffer[Int]()
        for (l <- p){
          if (v_hm.get(l).isDefined){
            out += l
          }
        }
        (x._1, x._2, x._3, out.toArray)
      })

    // model parameters
    val alpha = init._1
    val beta = init._2
    // ODM before balance
    val odm_bb = cost_filtred_path.map(model2(_,alpha, beta))
    odm_bb.persist()
    odm_bb.localCheckpoint()
    //cost_filtred_path.collect().map(x => println("0: "+x))

    val dK = balance(odm_bb)
    dK.persist()
    val vK = dK.flatMap(x => x._4.map(id => (id, x._3))).reduceByKey(_+_)
    vK.persist()
    val k = vK.map(x => v_hm(x._1)* x._2).reduce(_+_) / vK.map(x => math.pow(x._2,2)).reduce(_+_)
    println("param 'k' is "+ k)
    return dK.map(x => (x._1, x._2, k * x._3))
  }

  def model2(cell: (Zone, Zone, Double, Array[Int]), alpha: Double, beta: Double): (Zone, Zone, Double, Array[Int]) ={
    val c = cell._3
    val out_val = cell._1.trips * cell._2.trips * math.pow(c, alpha) * math.exp(- beta * c)
    return (cell._1, cell._2, out_val, cell._4)
  }

  /**
    * Method balances OD Matrix.
    * @param odm - OD Matrix RDD[(source, destination, trips, filtred_path)]
    * @param debug
    * @return OD Matrix
    */
  def balance(odm: RDD[(Zone, Zone, Double, Array[Int])], debug: Boolean = false): RDD[(Zone, Zone, Double, Array[Int])] = {
    odm.persist()
    val row_coef = odm.map(x => (x._1, x._3)).reduceByKey(_+_).map(x => (x._1, x._1.trips.toDouble / x._2.toDouble))
    row_coef.persist()
    val odm_ar = odm.map(x => (x._1, x)).join(row_coef).map(_._2).map(x => (x._1._1, x._1._2, x._1._3 * x._2))
    val col_coef = odm_ar.map(x => (x._2, x._3)).reduceByKey(_+_).map(x => (x._1, x._1.trips.toDouble / x._2.toDouble))

    val odm_ac = odm.map(x => (x._2, x)).join(col_coef).map(_._2).map(x => (x._1._1, x)).join(row_coef).map(_._2)
      .map(x => (x._1._1._1, x._1._1._2, x._1._1._3 * x._2 * x._1._2, x._1._1._4))
    odm_ac.persist()

    //compute average balance
    val row_coef2 = odm_ac.map(x => (x._1, x._3)).reduceByKey(_+_).map(x => x._1.trips.toDouble / x._2.toDouble)
    row_coef2.persist()
    //println(row_coef2.first())
    if (debug){
      println("Balance coef: "+row_coef2.reduce(_+_)/row_coef2.count())
    }
    return odm_ac
  }
}



