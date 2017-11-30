# parSketch
Massively Distributed Indexing of Time Series
<Source code for ...paper>

## Abstract 
This code is a Scala implementation of a sketch/random projection-based method to efficiently perform the parallel indexing of time series and similarity search on them.
 
The method is based on the use of random vectors. The basic idea is to multiply each time series with a set of random vectors. The result of that operation is a ”sketch” for each time series consisting of the distance (or similarity) of the time series to each random vector. Then two time series can be compared by comparing sketches.

Similar items are hashed to the same buckets (grid structures). Each grid consists the sketch values corresponding to a specific set of random vectors over all time series. 

## Getting Started
 
### Prerequisites

Resource Name | Resource Description | Supported Version  | Remarks
------------ | ------------- | ------------- | -------------
Oracle Java | The Java Runtime Environment (JRE) is a software package to run Java and Scala applications  |8.0
Apache Hadoop | Hadoop Distributed File System (HDFS™): A distributed file system that provides high-throughput access to application data  | v. 2.7.x 
Apache Spark | Large-scale data processing framework | v. 2.1.0 or later 
PostgreSQL | Relational database system to store indices and to provide more effective Query Processing on indexed data | v. 9.3.x or later| One instance of PostgreSQL server should be running on each node of a cluster. 



 


### Installing 

The code is presented as a Maven-built project. An executable jar with all dependencies can be built with the following command:

`mvn clean package
`

## Running

To run the project you can use both the bash script or directly run separate jars with spark-submit tool.  

By running bash script [parSketch_TSToDBMulti.sh](scripts/parSketch_TSToDBMulti.sh) you can perform the full cycle of the Index building on Time Series, including Data Generation, Grid (Index) Construction and Query Processing stages. Also it allows to run Grid Construction on a multiple input. 

<pre>
./parSketch_TSToDBMulti.sh [tsnum] [queries] [thresh] [input] [gridsize]
    
    Options:
    tsnum       [required]  [Int]               Number of Time Series
    queries     [required]  [String]            Path to Queries file
    thresh      [required]  [Float]             Given threshold to find candidate time series from the grids 
    input       [required]  [Int|String]        {number of folders for input data generation | path to the Input Time Series Data}
    gridsize    [optional]  [Int]               Size of Grid cell            
</pre>

Also you can run each stage as a separate call (running on a single input) with the list of required options:

<pre>
#Grid construction
    path_to_spark_bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti path_to_jar/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath $file --sizeSketches $num --gridDimension $num --gridSize $num --batchSize $num --gridConstruction true  --numPart $num --tsNum $tsnum --nodesFile
	
#Index creation
    path_to_spark_bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti path_to_jar/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath $file --sizeSketches 30 --gridDimension 2 --gridSize $gridSize --batchSize 10000  --numPart $numpart --tsNum $tsnum
    
#Query processing
    path_to_spark_bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti path_to_jar/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath ts_256_$tsnum --sizeSketches 30 --gridDimension 2 --gridSize $gridSize --queryFilePath queries_$queries --candThresh $thresh --numPart $numpart --tsNum $tsnum
    
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
    --candThresh            Given threshold to find candidate time series from the grids  
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


## Licence
Coming soon 