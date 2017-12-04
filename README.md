# spark-parSketch
Massively Distributed Indexing of Time Series for Apache Spark


## Abstract 
This code is a Scala implementation of a sketch/random projection-based method to efficiently perform the parallel indexing of time series and similarity search on them ([RadiusSketch: Massively Distributed Indexing of Time Series.pdf](https://hal-lirmm.ccsd.cnrs.fr/lirmm-01620154/file/ParSketch__DSAA_.pdf)).
 
The method is based on the use of random vectors. The basic idea is to multiply each time series with a set of random vectors. The result of that operation is a ”sketch” for each time series consisting of the distance (or similarity) of the time series to each random vector. Thus two time series can be compared by comparing sketches.

The approach uses a set of grid structures to hold the time series sketches. Each grid contains the sketch values corresponding to a specific set of random vectors over all time series. Two time series are considered to be similar if they are assigned to the same grid cell in a given fraction of grids.



## Getting Started
 
### Prerequisites

Resource Name | Resource Description | Supported Version  | Remarks
------------ | ------------- | ------------- | -------------
Oracle Java | The Java Runtime Environment (JRE) is a software package to run Java and Scala applications | 8.0
Apache Hadoop | Hadoop Distributed File System (HDFS™): A distributed file system that provides high-throughput access to application data | v. 2.7.x 
Apache Spark | Large-scale data processing framework | v. 2.1.0 or later 
PostgreSQL | Relational database system to store indices and to provide more effective Query Processing on indexed data | v. 9.3.x or later| One instance of PostgreSQL server should be running on each node of a cluster. 


### Installing 

The code is presented as a Maven-built project. An executable jar with all dependencies can be built with the following command:

`mvn clean package
`

## Running

To run the project you can either use the bash script or directly run separate jars with the spark-submit tool.  

By running the bash script [parSketch_TSToDBMulti.sh](scripts/parSketch_TSToDBMulti.sh) you can perform the full cycle of the Index building on Time Series, including Data Generation*, Grid (Index) Construction and Query Processing stages. It also allows to run Grid Construction on a multiple input. 


<pre>
./parSketch_TSToDBMulti.sh [tsnum] [queries] [thresh] [input] [gridsize]
    
    Options:
    tsnum       [required]  [Int]               Number of Time Series
    queries     [required]  [String]            Path to Queries file
    thresh      [required]  [Float]             Given threshold to find candidate time series from the grids 
    input       [required]  [Int|String]        {number of folders for input data generation | path to the Input Time Series Data}
    gridsize    [optional]  [Int]               Size of Grid cell 
    workdir     [optional]  [String]            Path to the dirrectory with jars, by default /tmp   
</pre>

*the jar for Data Generation must be in $workdir (see section Datasets)


Alternatively, you can run each stage as a separate call (running on a single input) with the list of required options:

<pre>
#Grid construction
    $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val --gridConstruction true  --numPart int_val --tsNum int_val --nodesFile path
	
#Index creation
    $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val  --numPart int_val --tsNum int_val
    
#Query processing
    $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --queryFilePath path --candThresh dec_val --numPart int_val --tsNum int_val
    
    Options:
    
    --tsFilePath            Path to the Time Series input file
    --sizeSketches          Size of Sketch [Default: 30] 
    --gridDimension         Dimension of Grid cell [Default: 2]
    --gridSize              Size of Grid cell [Default: 2]
    --tsNum                 Number of Time Series
    --batchSize             Size of Insert batch to DB [Default: 1000]
    --gridConstruction      Boolean parameter [Default: false]
    --numPart               Number of partitions for parallel data processing
    
    --queryFilePath         Path to a given collection of queries
    --candThresh            Given threshold (fraction) to find candidate time series from the grids  
    --queryResPath          Path to the result of the query (Optional)
    --saveResult            Boolean parameter, if true - save result of query to file, false - statistics output to console [Default: false]
    --nodesFile             Path to the list of cluster nodes (hostname ips) [Default: nodes]
    
    --gridsResPath          Path to the result of grids construction (Optional)
    --jdbcDriver            JDBC driver to RDB  [Default: PostgreSQL] (Optional)
    --jdbcUsername          Username to connect to DB (Optional)
    --jdbcPassword          Password to connect to DB (Optional)
</pre>


## Deployment 
An alternative script to deploy working Hadoop/Spark/PotgreSQL environment will be available soon in /scripts folder.


## Datasets 

For experimental verification synthetic datasets were used, generated by [Random Walk Time Series Generator](https://github.com/lev-a/RandomWalk-tsGenerator).  At each time point the generator draws a random number from a Gaussian distribution N(0,1), then adds the value of the last number to the new number.


## License
Apache License Version 2.0