package com.parser.source.hl7.helper

private[parser] case class Hl7Schema(version: String,
                                     sampleData: String,
                                     disableDefaultSchemaDefine: Option[String] = Option.empty[String])
