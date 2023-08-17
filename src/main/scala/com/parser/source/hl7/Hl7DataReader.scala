package com.parser.source.hl7

import com.cleanly
import com.parser.source.hl7.helper.Hl7Parser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.util.{Failure, Success}

class Hl7DataReader(options: Map[String, String])(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with Hl7Parser {
  override def sqlContext: SQLContext = sparkSession.sqlContext

  private val version: String = options.getOrElse("hl7version", "2.5")

  override def schema: StructType =
    StructType(Seq(
      StructField("value", StringType)
    ))

  override def buildScan(): RDD[Row] = readHL7DataFromSource().map(parseHL7Message)

  /**
   * read HL7 Data From Source
   *
   * @return
   */
  private def readHL7DataFromSource(): RDD[String] = {
    val path: String = options.getOrElse("path",
      throw new IllegalArgumentException("Path parameter is required for reading HL7 data."))
    cleanly(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration))(_.close)(fs => {
      val filePath = new Path(path)
      val fileStatus = fs.getFileStatus(filePath)
      if (fileStatus.isDirectory) {
        val files = fs.listStatus(filePath)
        val fileRDDs = files.map(file => {
          cleanly(fs.open(file.getPath))(_.close) { fileData =>
            scala.io.Source.fromInputStream(fileData).mkString
          }.get
        })
        sqlContext.sparkContext.parallelize(fileRDDs)
      } else {
        cleanly(fs.open(filePath))(_.close) { fileData =>
          sqlContext.sparkContext.parallelize(
            Seq(scala.io.Source.fromInputStream(fileData).mkString))
        }.get
      }
    }) match {
      case Failure(exception) => throw exception
      case Success(value) => value
    }
  }

  /**
   * Parse the Hl7 Data Into Json Message
   *
   * @param hl7Message - Each File Content
   * @return
   */
  private def parseHL7Message(hl7Message: String): Row = {
    Row(parserHl7MessageToJson(version, hl7Message))
  }
}
