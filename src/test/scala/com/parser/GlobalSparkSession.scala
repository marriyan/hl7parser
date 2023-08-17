package com

import org.apache.spark.sql.SparkSession

object GlobalSparkSession {
  private lazy val sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("GlobalSparkSession")
      .getOrCreate()
  }

  def getSession: SparkSession = sparkSession
}
