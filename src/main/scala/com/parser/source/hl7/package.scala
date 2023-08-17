package com.parser.source

import com.parser.source.hl7.helper.{Hl7Parser, Hl7Schema, SchemaHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

package object hl7 {


  object implicits {
    implicit class DataFrameTransforms(df: DataFrame) {
      /**
       * convert the Hl7 Json Data to Dataframe Struct Type.
       *
       * @param sparkSchema - Optional(Custom Spark Schema)
       * @param hl7Version  - Optional(HL7 Schema)
       * @return
       */
      def parse(sparkSchema: Option[StructType],
                hl7Version: Option[Hl7Schema]): DataFrame = {
        val sparkSession: SparkSession = df.sparkSession
        import sparkSession.implicits._
        val schema = getSchema(sparkSession, sparkSchema, hl7Version)
        df.
          withColumn("parse", from_json($"value", schema))
          .selectExpr("parse.*")
      }

      /**
       * Identify the Schema in Run time.
       *
       * @param sparkSession     - spark session object
       * @param sparkSchema      - Optional(Custom Spark Schema)
       * @param hl7VersionSchema - Optional(HL7 Schema)
       * @return
       */
      private def getSchema(sparkSession: SparkSession, sparkSchema: Option[StructType],
                            hl7VersionSchema: Option[Hl7Schema]): StructType = {
        sparkSchema.fold {
          val hl7Schema: Hl7Schema = hl7VersionSchema.
            getOrElse(throw
              new IllegalArgumentException("HL7 Schema message parameter is required for defining the schema."))
          val jsonData = Hl7Parser.parserHl7MessageToJson(hl7Schema.version, hl7Schema.sampleData)
          hl7Schema.disableDefaultSchemaDefine match {
            case Some(value) if value.toBoolean =>
              SchemaHelper.extractDynamicSchema(jsonData)
            case _ =>
              import sparkSession.implicits._
              sparkSession.read.json(Seq(jsonData).toDS).schema
          }
        }(identity)
      }
    }
  }
}
