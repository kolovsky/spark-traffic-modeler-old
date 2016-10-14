Traffic Volume Modelling in Parallel Computing Environment
==========================================================

This project is focused on traffic volume modelling in Apache Spark.
Now it is implemented 3 base step in 4-steps transportation model.

* Trip distribution
    * OD Matrix calibration
* Traffic assigmnet

Trip distibution
----------------
It is full parallel method. You can use: Gravity model, Log-normal, Top-log-normal distribution function.

OD Matrix calibration
-------------------
It was used method by Spiess (1990) and It was added better method for minimalization objective function (Z).
You can use 3 minimalization method:
* Gradient method with long step (used in Spiess 90)
* Conjugate gradient method - Fletcher-Reeves method
* Conjugate gradient method - Polak-Ribier method

Traffic assigment
------------------

Example running
-----------------
Network: > 3 000 000 edges
<br>Zones (centroids): > 22 000
<br>Cluster: 100 CPU (xenon)

<br> Trip distribution: < 60'
<br> Calibtate OD Matrix: 30' + 11'/iteration
<br> Traffic assigment: 38'

Future work
-------------
* DTA (Dinamic Traffic Assigment)

Example
--------
'''scala
val m_conf = new ModelConf()
      .set("length_coef", "0.7")
      .set("time_coef", "0.3")
      .set("alpha", "1")
      .set("beta", "-0.3")
      .set("gamma", "1")
      .set("F", "log_normal")

// dataset of edges (edge_id, source node, destination node, length, travel time)
val edges = Array((1, 1, 2, 10.0, 15.0),(2, 2, 3, 17.0, 13.0))

// dataset of zones (id, node_id, trips)
val zones = Array(new Zone(1, 1, 20), new Zone(2, 3, 30))

// dataset of count profile (edge_id, traffic count)
val counts = Array((1, 25.0),(2, 45.0))

val n = new NetworkIndex()
n.addEdges(edges)

val m = new Model(n, zones, m_conf)

// trip distribution
val odm = m.estimateODMatrix(sc.parallelize(zones))

// OD Matrix Calibration
val codm = m.calibrateODMatrix(odm, counts, 10, "PR")

//traffic assigment
val t = m.assigmentTraffic2(codm)
'''
