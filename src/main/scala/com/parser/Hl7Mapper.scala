package com.parser

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.parser.exeptions.MappingJsonFileMissingException
import com.parser.util.CompressionUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.JavaConverters._


trait Hl7Mapper {

  private def getColumnNamesWithDotNotation(schema: StructType,
                                            parentName: String = ""): Seq[String] = {
    schema.fields.flatMap { field =>
      val columnName = if (parentName.isEmpty) field.name else s"$parentName.${field.name}"
      field.dataType match {
        case structType: StructType =>
          getColumnNamesWithDotNotation(structType, columnName)
        case ArrayType(elementType, _) if elementType.isInstanceOf[StructType] =>
          getColumnNamesWithDotNotation(elementType.asInstanceOf[StructType], columnName)
        case _ =>
          Seq(columnName)
      }
    }
  }

  private def getSparkColumns(value: String): Set[String] = {
    val rx = "\\[(.*?)\\]".r
    val results = rx.findAllMatchIn(value).map(_ group 1)
    results.toSet
  }

  private def convertJsonToStruct(node: JsonNode,
                                  columns: Seq[String]): String = node.getNodeType match {
    case JsonNodeType.OBJECT =>
      val structFields = node.fields.asScala.map { entry =>
        s"'${entry.getKey}', ${convertJsonToStruct(entry.getValue, columns)}"
      }
      s"named_struct(${structFields.mkString(", ")})"

    case JsonNodeType.ARRAY =>
      val arrayItems = node.elements.asScala.map(m => convertJsonToStruct(m, columns))
      s"array(${arrayItems.mkString(", ")})"

    case JsonNodeType.STRING =>
      val sparkColumns = getSparkColumns(node.asText())
      if (sparkColumns.nonEmpty) {
        sparkColumns.foldLeft(node.asText()) { (acc, elem) =>
          acc.replaceAll(elem, if (columns.contains(elem)) elem else "null")
        }.replaceAll("\\[|\\]", "")
      } else {
        s"'${node.asText()}'"
      }
    case JsonNodeType.NUMBER | JsonNodeType.BOOLEAN | JsonNodeType.BINARY => node.asText()
    case _ => "null"
  }

  private def parseJson(jsonString: String): JsonNode = {
    val objectMapper = new ObjectMapper()
    objectMapper.readTree(jsonString)
  }

  def transform(df: DataFrame,
                conf: Map[String, String]): DataFrame = {
    val mappingJson = conf.getOrElse("mappingJson",
      throw MappingJsonFileMissingException("mappingJson config missing."))
    val columns = getColumnNamesWithDotNotation(df.schema)
    df.selectExpr(
      s"to_json(${convertJsonToStruct(parseJson(CompressionUtil.decompress(mappingJson)), columns)}) as value")
  }
}
