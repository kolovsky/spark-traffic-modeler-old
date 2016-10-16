package com.kolovsky.traffic_modeler

/**
 * Created by kolovsky on 19.5.16.
 */
class ModelConf extends Serializable{
  var length_coef: Double = 1
  var time_coef: Double = 1
  // pro distribucni funkce F
  var alpha: Double = 0
  var beta: Double = 0
  var gamma: Double = 0
  var F: Double => Double = F_log_normal

  def F_log_normal(x: Double): Double = {
    return alpha * math.exp(beta * math.log(x + 1))
  }
  def F_top_log_normal(x: Double): Double = {
    return alpha * math.exp(beta * math.log(x / gamma))
  }
  def F_exponencial(x: Double): Double = {
    return alpha * math.exp(beta * x)
  }
  def F_kvadratic(x: Double): Double = {
    return math.pow(x, -2)
  }
  def set(variable: String, value: String): ModelConf ={
    if (variable == "length_coef"){
      length_coef = value.toDouble
    }
    else if(variable == "time_coef"){
      time_coef = value.toDouble
    }
    else if(variable == "alpha"){
      alpha = value.toDouble
    }
    else if(variable == "beta"){
      beta = value.toDouble
    }
    else if(variable == "gamma"){
      gamma = value.toDouble
    }
    else if(variable == "F"){
      if (value == "log_normal"){
        F = F_log_normal
      }
      else if (value == "top_log_normal"){
        F = F_top_log_normal
      }
      else if (value == "exponencial"){
        F = F_exponencial
      }
      else if(value == "kvadratic"){
        F = F_kvadratic
      }
      else {
        throw new Exception("Distribution function with name "+value+" does not exist.")
      }
    }
    else {
      throw new Exception("Variable with name "+variable+" does not exist.")
    }
    return this
  }
}
