package com.kolovsky.traffic_modeler

import scala.collection.mutable.HashMap

/**
  * Created by kolovsky on 10.11.16.
  */
class Lagrange(sum_by_row: Array[(Zone, Double)], sum_by_column: Array[(Zone, Double)], all_trip: Double, limit: (Double, String, Int) => Double) extends Serializable {
  val all_trips = all_trip

  //Lagrange Coef
  var lc_beta_i = new HashMap[Zone, Double]()
  lc_beta_i ++= sum_by_row.map(x => (x._1, 0.0) )
  var lc_beta_j = new HashMap[Zone, Double]()
  lc_beta_j ++= sum_by_column.map(x => (x._1, 0.0) )
  var lc_delta_i = new HashMap[Zone, Double]()
  lc_delta_i ++= sum_by_row.map(x => (x._1, 0.0) )
  var lc_delta_j = new HashMap[Zone, Double]()
  lc_delta_j ++= sum_by_column.map(x => (x._1, 0.0) )
  var delta = 0.0
  var beta = 0.0

  // Limits
  var lim_beta_i = new HashMap[Zone, Double]()
  lim_beta_i ++= sum_by_row.map(x => (x._1, limit(x._2, "lo",1)))
  var lim_beta_j = new HashMap[Zone, Double]()
  lim_beta_j ++= sum_by_column.map(x => (x._1, limit(x._2, "lo",1)))
  var lim_delta_i = new HashMap[Zone, Double]()
  lim_delta_i ++= sum_by_row.map(x => (x._1, limit(x._2, "up",1)))
  var lim_delta_j = new HashMap[Zone, Double]()
  lim_delta_j ++= sum_by_column.map(x => (x._1, limit(x._2, "up",1)))

  // Lagrange Coeficients
  def getBetaRow(i: Zone): Double = {
    return lc_beta_i.get(i).get
  }
  def getBetaColumn(j: Zone): Double = {
    return lc_beta_j.get(j).get
  }
  def getBeta(): Double = {
    return  beta
  }
  def getDeltaRow(i: Zone): Double = {
    return lc_delta_i.get(i).get
  }
  def getDeltaColumn(j: Zone): Double = {
    return lc_delta_j.get(j).get
  }
  def getDelta(): Double = {
    return delta
  }

  // Limits
  def getLRow(i: Zone): Double = {
    return lim_beta_i.get(i).get
  }
  def getURow(i: Zone): Double = {
    return lim_delta_i.get(i).get
  }
  def getLColumn(j: Zone): Double = {
    return lim_beta_j.get(j).get
  }
  def getUColumn(j: Zone): Double = {
    return lim_delta_j.get(j).get
  }
  def getL(): Double = {
    return  limit(all_trips, "lo", 2)
  }
  def getU(): Double = {
    return limit(all_trips, "up", 2)
  }
  def updateLagrange(sum_by_row: Array[(Zone, Double)], sum_by_column: Array[(Zone, Double)], all_trip: Double): Unit ={
    for (row <- sum_by_row){
      //delta_i
      val new_delta_i = getURow(row._1) - row._2 + getDeltaRow(row._1)
      lc_delta_i.update(row._1, new_delta_i)
      //beta_i
      val new_beta_i = row._2 - getLRow(row._1) + getBetaRow(row._1)
      lc_beta_i.update(row._1, new_beta_i)
    }
    for (col <- sum_by_column){
      //delta_i
      val new_delta_j = getUColumn(col._1) - col._2 + getDeltaColumn(col._1)
      lc_delta_j.update(col._1, new_delta_j)
      //beta_i
      val new_beta_j = col._2 - getLColumn(col._1) + getBetaColumn(col._1)
      lc_beta_j.update(col._1, new_beta_j)
    }
    delta = getU() - all_trip + getDelta()
    beta = all_trip - getL() + getBeta()
  }
}
