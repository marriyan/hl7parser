package com.parser

import com.GlobalSparkSession
import com.parser.exeptions.ParamMissingException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class Hl7ConnectorSpec extends WordSpec with Matchers with MockFactory {
  val sparkSession: SparkSession = GlobalSparkSession.getSession

  "Hl7Connector" should {
    "handle invalid input parameters" in {
      val connector = new Hl7Connector {}
      val sourceType = Some("invalid_type")
      val sourceSystem = Some(Map(
        "username" -> "testuser",
        "password" -> "testpassword",
        "host" -> "sftp.example.com",
        "port" -> "22"))

      // Assert that an IllegalArgumentException is thrown
      an[IllegalArgumentException] should be thrownBy {
        connector.copyOperation(sparkSession, "/source/path", sourceType, sourceSystem)
      }
    }

    "copy files from SFTP to HDFS" in {
      val connector = new Hl7Connector {
        override private[com] def sftpToHdfsCopyActivity(conf: Configuration, sourcePath: String,
                                                         options: Map[String, String]): String = {
          options("username") shouldBe "testuser"
          options("password") shouldBe "testpassword"
          options("host") shouldBe "sftp.example.com"
          options("port") shouldBe "22"
          sourcePath shouldBe "/source/path"
          "/destination/path"
        }
      }
      val sourceType = Some("sftp")
      val sourceSystem = Some(Map("username" -> "testuser", "password" -> "testpassword",
        "host" -> "sftp.example.com", "port" -> "22"))

      val result = connector.copyOperation(sparkSession, "/source/path", sourceType, sourceSystem)

      result shouldBe Some("/destination/path")
    }
    "throw an exception when path parameter is missing" in {
      val connector = new Hl7Connector {}

      val options = Map("hl7schema" -> "compressedSchema", "hl7version" -> "2.7")
      val schema = Option.empty[StructType]

      // Verify that the expected exception is thrown
      assertThrows[ParamMissingException] {
        connector.readData(sparkSession, options, Some("sftp"), Some(Map()), schema)
      }
    }
  }

}
