Traffic Volume Modelling for Apache Spark
=========================================

Framework for road traffic modelling for Apache Spark. The framework is experimental without any optimizations! Please use only for research or testing!
2 basic steps are implemented form 4-steps transportation model now.

* Trip distribution (Gravity model and Estimation using traffic count)
* Traffic assignment (all-or-nothing method)

Trip distribution
----------------
Gravity model is implemented now. You can choose form several modifications.

OD Matrix calibration (estimation)
----------------------------------
You can use 2 method:

* method according Spiess 1990 with 3 method of minimalization (suitable for large models)
    * Gradient method with long step (used in Spiess 90)
    * Conjugate gradient method - Fletcher-Reeves method
    * Conjugate gradient method - Polak-Ribier method (fastest convergence rate)
* method according Doblas 2005 (not suitable for large models)

Doblas, J. & Benitez, F. G. An approach to estimating and updating origin--destination matrices based upon traffic counts
preserving the prior structure of a survey matrix
_Transportation Research Part B: Methodological, Elsevier, 2005, 39, 565-591_

Spiess, H. A gradient approach for the OD matrix adjustment problem, 1990, 1, 2

Traffic assignment
-----------------
All-or-nothing traffic assignment is implemented now. This method is suitable for uncongested network (macro network)

Example running
---------------
<b>Czech Republic transport model</b>
Network: > 3 000 000 edges
<br>Zones (centroids): > 22 000
<br>Cluster: 24 node, every node has 4 cores and 32 GB RAM

<br> Trip distribution: < 60'
<br> Calibrate OD Matrix (Spiess method): 30' + 11'/iteration
<br> Traffic assignment: 38'

<br><b>Europe transport model</b>
<br>Network: 5 mil. edges
<br>Zones (centroids): about 140 000
<br>Cluster: 24 node, every node has 4 cores and 32 GB RAM
<br> Application uptime was <b>30 hours</b>
<br> Search limit for Dijkstra's algorithm was 300 km

Future work
-------------
* equilibrium traffic assignment

Scala Documentation
-------------------

http://kolovsky.com/scala-doc/

Example
--------
```scala
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val m_conf = new ModelConf()
      .set("length_coef", "0.7")
      .set("time_coef", "0.3")
      .set("alpha", "1") // coefficients for deterrence function
      .set("beta", "-0.3")
      .set("gamma", "1")
      .set("F", "log_normal") // set deterrence function (will be used in 'estimateODMatrix' method)
      .set("searchRadius","50") // set search limit for Dijkstra's algorithm

    // dataset of edges (edge_id, source node, destination node, length, travel time)
    val edges = Array((1, 1, 2, 10.0, 15.0, false),(2, 2, 3, 17.0, 13.0, false))

    // dataset of zones (id, node_id, trips)
    val zones = Array(new Zone(1, 1, 20), new Zone(2, 3, 30))

    // dataset of count profile (edge_id, traffic count)
    val counts = Array((1, 25.0),(2, 45.0))

    // create graph from edges
    val n: Network = new NetworkIndex()
    n.addEdges(edges)

    // model
    val m = new Model(n, zones, m_conf)

    // trip distribution
    val cost = m.costMatrix(sc.parallelize(zones))
    val odm = m.estimateODMatrix(cost)

    // or with new method
    val odm2 = m.estimateODMatrixK(sc.parallelize(zones), (0, 0.012, 1), counts)

    // OD Matrix Calibration (Spiess 1990)
    val codm = m.calibrateODMatrix(odm, counts, 10, "PR")

    //traffic assigment
    val t = m.assigmentTraffic2(codm)
```
