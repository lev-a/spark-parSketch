// scalastyle:off println
package fr.inria.zenith.adt

import java.io.File

import org.apache.commons.cli.{BasicParser, CommandLine, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._
import scala.util.control.Breaks._


/**
  * Usage:
  * Grid construction
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val --gridConstruction true  --numPart int_val --tsNum int_val --nodesFile path
	*
  * Index creation
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val  --numPart int_val --tsNum int_val
  *
  * Query processing
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --queryFilePath path --candThresh dec_val --numPart int_val --tsNum int_val
  */
object TSToDBMulti {

  // Product of vector and matrix
  def mult[A](a: Array[A], b: Array[Array[A]])(implicit n: Numeric[A]) = {
    import n._
    for (col <- b)
      yield
        a zip col map Function.tupled(_*_) reduceLeft (_+_)
  }

  //Random vectors Array[Array[Floats]] generator where a  = size of Sketches, b = size of Random vector
  def ranD(a: Int, b: Int)= {
    (for (j<-0 until a) yield
      (for (i <- 0 until b) yield (scala.util.Random.nextInt(2) * 2 - 1).toFloat).toArray).toArray
  }

  def distanceNorm(xs: (Array[Float],Float,Float), ys:(Array[Float],Float,Float)) =
    sqrt((xs._1 zip ys._1).map { case (x,y) => pow((y - ys._2)/ys._3 - (x - xs._2)/xs._3, 2)}.sum)

  def distance(xs: Array[Float], ys: Array[Float]) =
    sqrt((xs zip ys).map { case (x,y) => pow(y-x, 2)}.sum)

  def main(args: Array[String]): Unit = {
    // Command line parameters
    val options = new Options()
    options.addOption("tsFilePath", true, "Path to the Time Series input file")
    options.addOption("tsNum", true, "Number of input Time Series  [Default: 4] (Optional)")
    options.addOption("sizeSketches", true, "Size of Sketch [Default: 30]")
    options.addOption("gridDimension", true, "Dimension of Grid cell [Default: 2]")
    options.addOption("gridSize", true, "Size of Grid cell [Default: 2]")

    options.addOption("numPart", true, "Number of partitions for parallel data processing [Default: 8]")
    options.addOption("queryFilePath", true, "Path to a given collection of queries")
    options.addOption("candThresh", true, "Given threshold (fraction) to find candidate time series from the grids [Default: 0.6] ")
    options.addOption("gridConstruction", true, "Boolean parameter [Default: false]")

    options.addOption("batchSize", true, "Size of Insert batch to DB [Default: 1000]")
    options.addOption("nodesFile", true, "Path to the list of cluster nodes (hostname ips) [Default: nodes]")

    //optional
    options.addOption("numGroupsMin", true, "")
    options.addOption("gridsResPath", true, "Path to the result of grids construction")
    options.addOption("jdbcUsername", true, "Username to connect DB (Optional)")
    options.addOption("jdbcPassword", true, "Password to connect DB (Optional)")
    options.addOption("jdbcDriver", true, "JDBC driver to RDB (Optional) [Default: PostgreSQL]")
    options.addOption("queryResPath", true, "Path to the result of the query")
    options.addOption("saveResult", true, "Boolean parameter [Default: true]")
    options.addOption("topCand", true, "Number of top candidates to save  [Default: 10]")

    val clParser = new BasicParser()
    val cmd: CommandLine = clParser.parse(options, args)

    val tsFilePath = cmd.getOptionValue("tsFilePath", "")
    val tsNum = cmd.getOptionValue("tsNum").toInt
    val sizeSketches = cmd.getOptionValue("sizeSketches", "30").toInt
    val gridDimension = cmd.getOptionValue("gridDimension", "2").toInt
    val gridSize = cmd.getOptionValue("gridSize", "2").toInt
    val numPart = cmd.getOptionValue("numPart", "16").toInt

    val gridsResPath = cmd.getOptionValue("gridsResPath", "ts_gridsdb" + "_" + tsNum + "_" + sizeSketches + "_" + gridDimension + "_" + gridSize)

    val queryFilePath = cmd.getOptionValue("queryFilePath", "")
    val gridConstruction = cmd.getOptionValue("gridConstruction", "false").toBoolean
    val queryResPath = cmd.getOptionValue("queryResPath", queryFilePath + "_result")
    val saveResult = cmd.getOptionValue("saveResult", "true").toBoolean
    val candThresh = cmd.getOptionValue("candThresh", "0").toFloat
    val topCand = cmd.getOptionValue("topCand", "10").toInt

    val numGroups = sizeSketches / gridDimension

    /** PATHs **/
    val RndMxPath = new Path(gridsResPath + "/config/RndMxGrids")
    val urlHostsPath = new Path(gridsResPath + "/config/urlListDB")

    val conf: SparkConf = new SparkConf().setAppName("Time Series Grid Construction")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(classOf[Array[Float]], classOf[Array[String]], classOf[Array[Array[Float]]], classOf[scala.collection.mutable.ArraySeq[Float]], classOf[Array[Object]], classOf[scala.collection.immutable.Vector[Float]], Class.forName("[Lorg.apache.spark.util.collection.CompactBuffer;"), Class.forName("[Lscala.reflect.ClassTag$$anon$1;"), Class.forName("[I"), Class.forName("[B"), Class.forName("java.util.HashMap"), Class.forName("scala.collection.mutable.WrappedArray$ofRef")))
    val sc: SparkContext = new SparkContext(conf)

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    val t0 = System.currentTimeMillis()

    val rdbStorage = new Storage(cmd)

    /** Storage Initialization **/
    val urlList = hdfs.exists(urlHostsPath) match {
      case true => sc.objectFile[String](urlHostsPath.toString).collect().toList
      case false =>
        val urlList = rdbStorage.initialize()
        sc.parallelize(urlList).saveAsObjectFile(urlHostsPath.toString)
        urlList
    }

    /** Grid Construction **/
    if (!hdfs.exists(new Path(gridsResPath + "/config/RndMxGrids")) || gridConstruction) {

      println("Grid Construction stage for input: " + tsFilePath)

      val distFile = sc.objectFile[(Long, Array[Float])](tsFilePath, numPart)

      val distFileWithStats = distFile.map(t => (t._1, t._2, t._2.sum/t._2.length, t._2.map(x=>x*x).sum/t._2.length)).map(t => (t._1, t._2, t._3, sqrt(t._4-t._3*t._3).toFloat))
      val distFileNorm = distFileWithStats.map(t => (t._1, t._2.map(x => (x-t._3)/t._4)))

      val RandMx = hdfs.exists(RndMxPath) match {
        case true => sc.objectFile[Array[Float]](RndMxPath.toString).collect()
        case false => {
          val sizeTS = distFile.first()._2.length
          val RandMx = ranD(sizeSketches, sizeTS)
          sc.parallelize(RandMx).saveAsObjectFile(RndMxPath.toString)
          RandMx
        }
      }

      val RandMxBroad = sc.broadcast(RandMx)

      val res = distFileNorm.map(t => (t._1, mult(t._2, RandMxBroad.value)))

      val groups2 = res.flatMap(sc => sc._2.sliding(gridDimension, gridDimension)
          .zipWithIndex
          .map(group => ((group._2, group._1.map(v => (v / gridSize).toInt - (if (v < 0) 1 else 0)).toArray), sc._1)))


      groups2.foreachPartition(rdbStorage.insertPartition(_, urlList))

      val t1 = System.currentTimeMillis()
      println("Grids construction for input: "+ tsFilePath + " (Elapsed time): " + (t1 - t0)  + " ms")
    }

    /** Index Construction **/
    if (!gridConstruction && candThresh==0) {
      println("Index construction stage")
      val t1 = System.currentTimeMillis()

      rdbStorage.indexingGrids(urlList)

      val t2 = System.currentTimeMillis()
      println("Index construction (Elapsed time): " + (t2 - t1)  + " ms")
    }

    /** Query  Processing **/
    if (queryFilePath!= "" && candThresh>0) {
      println("Query processing stage")
      val t3 = System.currentTimeMillis()


      val RandMxGrids = sc.objectFile[Array[Float]](RndMxPath.toString).collect()

      val urlListFile = sc.objectFile[String](urlHostsPath.toString).collect().toList

      val executors = sc.getExecutorMemoryStatus.size
      val coresPerEx = sc.getConf.getInt("spark.executor.cores", 8)

      val queryFile = sc.objectFile[(Long, (Array[Float]))](queryFilePath, executors * coresPerEx*10)

      val queryFileWithStats = queryFile.map(t => (t._1, t._2, t._2.sum/t._2.length, t._2.map( x => x*x ).sum/t._2.length)).map(t => (t._1, (t._2, t._3, sqrt(t._4-t._3*t._3).toFloat)))
      val queryFileNorm = queryFileWithStats.map(t => (t._1, t._2._1.map( x => (x - t._2._2) / t._2._3 )))


      val queryGrids = queryFileNorm
        .map(t => (t._1, mult(t._2, RandMxGrids)))
        .flatMap(sc => sc._2.sliding(gridDimension, gridDimension).zipWithIndex.map(group => (group._2, (group._1.map(v => (v / gridSize).toInt - (if (v < 0) 1 else 0)).toArray, sc._1))))
        .repartition(math.max(executors * coresPerEx * numGroups, numPart*10) )

      val Query = queryGrids.mapPartitions(rdbStorage.queryDB(_, urlListFile))

      val queryRes = Query.reduceByKey(_ + _)

      /** Saving query result to file or to console **/
      if (saveResult) {
        val queryResFlt: RDD[((Long,Long),Int)] = queryRes.filter(_._2 > (candThresh * (sizeSketches / gridDimension)).toInt)
        if (!queryResFlt.isEmpty){
          val t4 = System.currentTimeMillis()
          println("Query processing (Elapsed time): " + (t4 - t3) + " ms")

          val inputs = new File(tsFilePath).exists() match {
            case true => scala.io.Source.fromFile(tsFilePath).getLines().mkString(",")
            case false => tsFilePath
          }

          val distFile = sc.objectFile[(Long, (Array[Float]))](inputs, numPart)
          val distFileWithStats = distFile.map(t => (t._1, t._2, t._2.sum/t._2.length, t._2.map( x => x*x ).sum/t._2.length)).map(t => (t._1, (t._2, t._3, sqrt(t._4-t._3*t._3).toFloat)))

          val jointRes = queryResFlt
              .map(v =>(v._1._2, v._1._1))
              .join(distFileWithStats)
              .map(v => (v._2._1, (v._1, v._2._2)))
              .groupByKey
              .join(queryFileWithStats)
              .map(v => ((v._1, v._2._2._1), v._2._1.map(ts => (ts._1, ts._2._1, distanceNorm(ts._2,v._2._2))).toSeq.sortWith(_._3 < _._3).take(topCand)))
              .map(v => ((v._1._1, v._1._2.mkString("[",",","]")),v._2.map(c => (c._1,c._2.mkString("[",",","]"))-> c._3 )))


          /**  Storing the candidates, sorted by Euclidean distance, to the text file  **/
          import java.io._
          val resPath = tsFilePath +"_res_" + System.currentTimeMillis() + "_" + (System.currentTimeMillis() - t3)
          val pw = new PrintWriter(new File(resPath))
          pw.write(jointRes.map(_.toString).reduce(_ + '\n' + _))
          pw.write("\n")
          pw.close
          println("Result saved to " + resPath)
        }
        else println ("No candidates found.")
      }
      else {
        breakable {
          for (i <- 0.1 until candThresh by 0.1) {
            val queryResFlt = queryRes.filter(_._2 > (i * (sizeSketches / gridDimension)).toInt)
            println(s"candThresh = $i, candidates = " + queryResFlt.count)
            if (queryResFlt.count <= 0) break
          }
        }
      }

      val t5 = System.currentTimeMillis()
      println("QP + Save res  (Elapsed time): " + (t5 - t3) + " ms")
    }
   sc.stop()
  }
}
// scalastyle:on println