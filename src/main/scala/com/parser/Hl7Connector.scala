package com.parser

import com.fs.domain.{HdfsSourceConfig, SFTPSourceConfig}
import com.fs.{HdfsFileSystem, SFTPFileSystem}
import com.parser.exeptions.{ParamMissingException, SchemaFilePathMissingException}
import com.parser.source.hl7.helper.Hl7Schema
import com.parser.util.{CompressionUtil, FileCopier}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID

trait Hl7Connector {

  private[com] def copyOperation(spark: SparkSession,
                            srcPath: String,
                            sourceType: Option[String] = Option.empty[String],
                            sourceSystem: Option[Map[String, String]]): Option[String] = {
    (sourceType, sourceSystem) match {
      case (Some(srcType), Some(sourceSys))
        if srcType.equalsIgnoreCase("sftp") =>
        Some(sftpToHdfsCopyActivity(spark.sparkContext.hadoopConfiguration,
          srcPath, sourceSys))
      case (Some(srcType), Some(sourceSys)) if List("local","hdfs","s3","blob","adls").contains(srcType.toLowerCase) =>
        val config = spark.sparkContext.hadoopConfiguration
        sourceSys.foreach(f => {
          config.set(f._1, f._2)
        })
        Option.empty
      case (None, None) => Option.empty
      case _ => throw new IllegalArgumentException(s"Illegal:($sourceType) passed.")
    }
  }

  private[com] def sftpToHdfsCopyActivity(conf: Configuration, sourcePath: String,
                                     options: Map[String, String]): String = {
    val username = options.getOrElse("username",
      throw ParamMissingException("username: parameter missing"))
    val password = options.getOrElse("password",
      throw ParamMissingException("password: parameter missing"))
    val host = options.getOrElse("host",
      throw ParamMissingException("host: parameter missing"))
    val port = options.getOrElse("port",
      throw ParamMissingException("port: parameter missing"))
    val sftp = new SFTPFileSystem(SFTPSourceConfig(host, port,
      username, password))
    val hdfs = new HdfsFileSystem(HdfsSourceConfig(conf))
    val copy = new FileCopier(sftp, hdfs)
    val destinationPath = s"/tmp/${UUID.randomUUID}"
    val path = copy.copy(sourcePath, destinationPath)
    hdfs.deleteOnExit(destinationPath)
    path
  }

  /**
   * read Hl7 Data Convert into DataFrame
   *
   * @param spark   - spark Session variable
   * @param options - Spark Expected Options
   * @param schema  - Custom Schema
   * @return
   */
  def readData(spark: SparkSession,
               options: Map[String, String],
               sourceType: Option[String] = Option.empty[String],
               sourceSystem: Option[Map[String, String]] = Option.empty,
               schema: Option[StructType] = Option.empty[StructType]): DataFrame = {
    val srcPath = options.getOrElse("path",
      throw ParamMissingException("path: parameter missing"))
    val updatedOptions = copyOperation(spark, srcPath,
      sourceType, sourceSystem) match {
      case Some(x) => options +
        ("path" -> x)
      case None => options
    }
    val hl7Schema: Option[Hl7Schema] = options.get("hl7schema") match {
      case Some(hl7schema) =>
        Some(Hl7Schema(options.getOrElse("hl7version", "2.5"),
          CompressionUtil.decompress(hl7schema)))
      case None if schema.isEmpty =>
        throw SchemaFilePathMissingException("hl7schema: parameter Missing.")
    }
    import com.parser.source.hl7.implicits._
    spark.read.format("com.parser.source.hl7")
      .options(updatedOptions)
      .load().parse(schema, hl7Schema)
  }
}
