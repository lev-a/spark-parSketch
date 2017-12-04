package fr.inria.zenith.adt

import java.sql.Connection

import org.apache.commons.cli.CommandLine
import org.postgresql.util.PSQLException

/**
  * Class to connect external storage (relational database). Provides methods to create connection with DB server, to insert values (grids), to index inserted items and to perform queries on them.
  */
class Storage (val cmd: CommandLine)  extends Serializable {

  private val jdbcUsername = cmd.getOptionValue("jdbcUsername", "postgres")
  private val jdbcPassword = cmd.getOptionValue("jdbcPassword", "postgres")
  private val batchSize = cmd.getOptionValue("batchSize", "1000").toInt
  private val nodesFile = cmd.getOptionValue("nodesFile", "nodes")
  private val jdbcDriver = cmd.getOptionValue("jdbcDriver", "org.postgresql.Driver")
  private val tsNum = cmd.getOptionValue("tsNum").toInt
  private val gridDimension = cmd.getOptionValue("gridDimension", "2").toInt
  private val sizeSketches = cmd.getOptionValue("sizeSketches", "4").toInt
  private val gridSize = cmd.getOptionValue("gridSize", "2").toInt
  private val gridsResPath = cmd.getOptionValue("gridsResPath", "ts_gridsdb" + "_" + tsNum + "_" + sizeSketches + "_" + gridDimension + "_" + gridSize)
  private val numGroups = sizeSketches/gridDimension
  private val columns : List[String] = "id_gr" :: List.tabulate(gridDimension)(n => "dim_" + (n + 1).toString) ::: List("id_ts")

  def connDB ( jdbcUrl: String): Connection  = {
    var connection:Connection = null

    try {
      Class.forName(jdbcDriver)
      import java.sql.DriverManager
      connection = DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword)
    } catch {
      case e : PSQLException => println(e.getMessage)
    }
    if (connection == null) println("connection is null")
    connection
  }

  def initialize() : List[String] =   {
    /********  Create dbs and tables  ********/
    println("Table name: " + gridsResPath)
    val createTbl = "create table " + gridsResPath + " (" + columns.mkString(" int, ") + " bigint)"
    val dropTbl = "drop table if exists " + gridsResPath

    val nodes = scala.io.Source.fromFile(nodesFile).getLines().toList

    val urlList = for (i <- 0 until numGroups) yield {
      val node = nodes(i % nodes.size)
      val host = s"jdbc:postgresql://$node/"
      val dbname = gridsResPath + "_group_" + i
      val conn = connDB( host + "postgres")
      val stmt = conn.createStatement
      stmt.executeUpdate("drop database if exists " + dbname)
      try {
        stmt.executeUpdate("create database " + dbname)
      }
      catch {
        case e: PSQLException => println(e.getMessage)
        case _ => println(host + dbname + " created...")
      }

      stmt.close()
      conn.close()

      val url = host + dbname + "?useServerPrepStmts=false&rewriteBatchedStatements=true"
      val connCreate = connDB(url)
      val stmtCreate = connCreate.createStatement()
      stmtCreate.executeUpdate(dropTbl)
      stmtCreate.executeUpdate(createTbl)

      stmtCreate.close()
      connCreate.close()

      url
    }
    urlList.toList
  }

  private val insertSql = "INSERT INTO " + gridsResPath + " VALUES (" + "?, " * (gridDimension + 1) + "?)"

  private val batches = batchSize * numGroups

  def insertPartition(part : Iterator[((Int, Array[Int]),Long)], urlList: List[String]) : Unit =  {

    val connList = urlList.map(connDB)
    val stmtList = connList.map(_.prepareStatement(insertSql))

    part.grouped(batches).foreach { batch =>
      batch.foreach { item =>
        stmtList(item._1._1).setInt(1, item._1._1)
        item._1._2.zipWithIndex.foreach(d => stmtList(item._1._1).setInt(d._2 + 2, d._1))
        stmtList(item._1._1).setLong(gridDimension + 2, item._2)
        stmtList(item._1._1).addBatch()
      }
      try {
        stmtList.map(_.executeBatch)
      } catch {
        case e : PSQLException => println(e.getMessage)
      }
    }
    stmtList.foreach(_.close)
    connList.foreach(_.close)
  }

  def indexingGrids( urlList: List[String]) : Unit =  {

    val createIdx = "CREATE INDEX ON " + gridsResPath + columns.mkString(" (", ", ", ")")

    val connList = urlList.map(connDB)
    val stmtList  = connList.map(_.createStatement)
    stmtList.par.foreach(_.execute(createIdx))
    stmtList.foreach(_.close)
    connList.foreach(_.close)
  }

  private val selectSql = "SELECT id_ts FROM " + gridsResPath + " WHERE" + columns.take(gridDimension + 1).mkString(" ", "=? and ", "=?")

  def queryDB(part : Iterator[(Int, (Array[Int],Long))], urlListFile: List[String]) : Iterator[((Long,Long),Int)] = {

    val connList = urlListFile.map(connDB)
    val stmtList = connList.map(_.prepareStatement(selectSql))

    val r = part.flatMap { item =>
      stmtList(item._1).setInt(1, item._1)
      item._2._1.zipWithIndex.foreach(d => stmtList(item._1).setInt(d._2 + 2, d._1))
      val rs = stmtList(item._1).executeQuery()
      Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(res => ((item._2._2, res._2.getLong(1)), 1))
    }.toList

    stmtList.foreach(_.close)
    connList.foreach(_.close)

    r.iterator
  }

}
