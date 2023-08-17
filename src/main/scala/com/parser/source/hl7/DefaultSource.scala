package com.parser.source.hl7

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends DataSourceRegister
  with RelationProvider with Logging {
  override def shortName(): String = "hl7"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new Hl7DataReader(parameters)(sqlContext.sparkSession)
  }
}
