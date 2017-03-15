package com.kolovsky.traffic_modeler

import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * Class for estimating OD Matrix. Least Square Method is used for determining distributed (deterrence) function  parameters.
  * Created by kolovsky on 10.12.16.
  */
class SimplexEstimator(RDD_zones: RDD[Zone], initParam: (Double, Double, Double), v: Array[(Int, Double)], m: Model, maxIter: Int, simplexSize: Double) extends ODMatrixEstimator with Serializable {
  var log = ""
  var cp: RDD[(Zone, Zone, Double, Array[Int])] = null
  var v_hm: HashMap[Int, Double] = null

  /**
    * Estimate OD Matrix
    * @return OD Matrix RDD[(source, destination, trips, path)]
    */
  def estimate(): RDD[(Zone, Zone, Double, Array[Int])] = {
    // hash map of counts
    v_hm = new HashMap[Int, Double]()
    v_hm ++= v
    //cost matrix
    val zones = RDD_zones.collect()
    cp = RDD_zones
      .flatMap(z => m.network.getPaths(z, zones, m.conf.length_coef, m.conf.time_coef, m.conf.searchRadius))
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
    cp.persist()
    cp.localCheckpoint()

    // simplex method
    val simplex = Array.ofDim[(Double, Double, Double)](4)
    simplex(0) = (0.0,0.0,0.0)
    val d = simplexSize
    val q = (d / (math.sqrt(2) * 3)) * (1 + math.sqrt(4))
    val p = (d / (math.sqrt(2) * 3)) * (- 2 + math.sqrt(4))

    simplex(1) = (p,q,q)
    simplex(2) = (q,p,q)
    simplex(3) = (q,q,p)
    /*for (s <- simplex){
      for (sv <- simplex){
        val dis = math.sqrt( math.pow(s._1 - sv._1,2) + math.pow(s._2 - sv._2,2) + math.pow(s._3 - sv._3,2))
        println(dis)
      }
    }*/
    for (i <- 0 to 3){
      val p = simplex(i)
      simplex(i) = (p._1 + initParam._1*10, p._2 + initParam._2*10, p._3 + initParam._3)
    }
    val arrZ = Array.ofDim[Double](4)
    var max = 0.0
    var min = 0.0
    var maxi = -1
    var mini = -1
    var old_maxi = -1
    for (i <- 0 to 3){
      arrZ(i) = fZ(simplex(i))
    }
    var j = 0
    while (j < maxIter){
      max = Double.NegativeInfinity
      min = Double.PositiveInfinity
      old_maxi = maxi
      for(i <- 0 to 3){
        if (max < arrZ(i)){
          max = arrZ(i)
          maxi = i
        }
        if (min > arrZ(i)){
          min = arrZ(i)
          mini = i
        }
      }
      if (maxi == old_maxi){
        println()
        println("change simplex size to: " + (0.5 * d))
        changeD(simplex, mini)
        for (i <- 0 to 3) {
          arrZ(i) = fZ(simplex(i))
        }
        maxi = -1
        mini = -1
      }
      else{
        var tx = 0.0
        var ty = 0.0
        var tz = 0.0
        for (i <- 0 to 3){
          if (i != maxi){
            tx += 1/3.0 * simplex(i)._1
            ty += 1/3.0 * simplex(i)._2
            tz += 1/3.0 * simplex(i)._3
          }
        }
        val nx = 2*tx - simplex(maxi)._1
        val ny = 2*ty - simplex(maxi)._2
        val nz = 2*tz - simplex(maxi)._3
        simplex(maxi) = (nx, ny, nz)
        arrZ(maxi) = fZ(simplex(maxi))
        print("\r it : "+j+" Z: "+math.sqrt(min/v.length)+" p: "+simplex(mini))
        j += 1
      }
    }
    println()
    min = Double.PositiveInfinity
    for(i <- 0 to 3){
      if (min > arrZ(i)){
        min = arrZ(i)
        mini = i
      }
    }
    return getODM(simplex(mini))
  }
  def getLog(): String = {
    return log
  }
  def addToLog(s: String): Unit ={
    log += s
  }

  /**
    * Get value of objective function.
    * @param param deterrence function parameters (alpha, beta, k)
    * @return value of objective function
    */
  private def fZ(param: (Double, Double, Double)): Double ={
    val odm = getODM(param)
    val v_m = odm.flatMap(x => x._4.map(id => (id, x._3))).reduceByKey(_+_)
    val z = v_m.map(x => math.pow(x._2 - v_hm(x._1), 2) ).reduce(_+_)
    return z
  }

  /**
    * Get ODM.
    * @param param deterrence function parameters (alpha, beta, k)
    * @return OD Matrix RDD[(source, destination, trips, filtred_path)]
    */
  private def getODM(param: (Double, Double, Double)): RDD[(Zone, Zone, Double, Array[Int])] = {
    val alpha = param._1/10
    val beta = param._2/10
    val k = param._3
    val odm_bb = cp.map(m.model2(_,alpha, beta))
    val odm = m.balance(odm_bb).map(x => (x._1, x._2, k * x._3, x._4))
    return odm
  }

  /**
    * Change simplex size
    * @param simplex
    * @param mini
    */
  private def changeD(simplex: Array[(Double, Double, Double)], mini: Int): Unit ={
    val min_p = simplex(mini)
    var p: (Double, Double, Double) = null
    for (i <- 0 to 3){
      if (mini != i){
        p = simplex(i)
        val nx = (p._1 - min_p._1)*0.5 + min_p._1
        val ny = (p._2 - min_p._2)*0.5 + min_p._2
        val nz = (p._3 - min_p._3)*0.5 + min_p._3
        simplex(i) = (nx, ny, nz)
      }
    }
  }
}
