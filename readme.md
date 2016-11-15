Traffic Volume Modelling in Parallel Computing Environment
==========================================================

This project is focused on traffic volume modelling in Apache Spark.
Now it is implemented 3 base step in 4-steps transportation model.

* Trip distribution
    * OD Matrix calibration
* Traffic assignment

Trip distribution
----------------
It is full parallel method. You can use: Gravity model, Log-normal, Top-log-normal distribution function.

OD Matrix calibration
---------------------
You can use 2 method:

* method according Spiess 1990 with 3 method of minimalization
    * Gradient method with long step (used in Spiess 90)
    * Conjugate gradient method - Fletcher-Reeves method
    * Conjugate gradient method - Polak-Ribier method
* method according Doblas 2005

Doblas, J. & Benitez, F. G. An approach to estimating and updating origin--destination matrices based upon traffic counts
preserving the prior structure of a survey matrix
_Transportation Research Part B: Methodological, Elsevier, 2005, 39, 565-591_

Spiess, H. A gradient approach for the OD matrix adjustment problem, 1990, 1, 2

Traffic assignment
-----------------
Static traffic assignment is implemented now.

Example running
-----------------
Network: > 3 000 000 edges
<br>Zones (centroids): > 22 000
<br>Cluster: 100 CPU (xenon)

<br> Trip distribution: < 60'
<br> Calibrate OD Matrix (Spiess method): 30' + 11'/iteration
<br> Traffic assignment: 38'

Future work
-------------
* Dynamic Traffic Assignment

Scala Documentation
-------------------

http://kolovsky.com/scala-doc/

Example
--------
```scala
    val sc = new SparkContext(conf)
    val m_conf = new ModelConf()
      .set("length_coef", "0.7")
      .set("time_coef", "0.3")
      .set("alpha", "1")
      .set("beta", "-0.3")
      .set("gamma", "1")
      .set("F", "log_normal")

    // dataset of edges (edge_id, source node, destination node, length, travel time, is oneway)
    val edges = Array((1, 1, 2, 10.0, 15.0, false),(2, 2, 3, 17.0, 13.0, false))

    // dataset of zones (id, node_id, trips)
    val zones = Array(new Zone(1, 1, 20), new Zone(2, 3, 30))

    // dataset of count profile (edge_id, traffic count)
    val counts = Array((1, 25.0),(2, 45.0))

    val n: Network = new NetworkIndex()
    n.addEdges(edges)

    val m = new Model(n, zones, m_conf)

    // trip distribution
    val cost = m.costMatrix(sc.parallelize(zones))
    val odm = m.estimateODMatrix(cost)

    // OD Matrix Calibration (Spiess 1990)
    val codm = m.calibrateODMatrix(odm, counts, 10, "PR")

    //traffic assigment
    val t = m.assigmentTraffic2(codm)
```
