package com.parser.source.hl7.helper

import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.parser.exeptions.InvalidSchemaJsonException
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[hl7] trait SchemaHelper extends Serializable {
  /**
   * Parse the Json String to JsonNode
   *
   * @param jsonString - Json String
   * @return
   */
  private def parseJson(jsonString: String): JsonNode = {
    val objectMapper = new ObjectMapper()
    objectMapper.readTree(jsonString)
  }

  /**
   * Parse the Json String to JsonNode
   *
   * @param schemaJson - Json String
   * @return
   */
  def extractDynamicSchema(schemaJson: String): StructType = {
    parseJson(schemaJson) match {
      case objectNode: ObjectNode => extractObjectSchema(objectNode.fields().asScala)
      case _ => throw InvalidSchemaJsonException("Invalid Schema Json File passed.")
    }
  }

  /**
   * Convert JsonNode to Spark DataType
   *
   * @param node - JsonNode
   * @return
   */
  private def getDataType(node: JsonNode): DataType = {
    node match {
      case objectNode: ObjectNode => extractObjectSchema(objectNode.fields().asScala)
      case arrayNode: ArrayNode =>
        extractArraySchema(arrayNode.fields().asScala)
      case _: TextNode => StringType
      case _: BinaryNode => BinaryType
      case _: BooleanNode => BooleanType
      case _: DoubleNode => DoubleType
      case _: FloatNode | _: DecimalNode => FloatType
      case _: IntNode | _: ShortNode | _: BigIntegerNode => IntegerType
      case _: LongNode => LongType
      case _ => StringType
    }
  }

  /**
   * Convert JsonObject to StructType
   *
   * @param iterator - iterator
   * @return
   */
  private def extractObjectSchema(iterator: Iterator[java.util.Map.Entry[String, JsonNode]]): StructType = {
    val fields = iterator.toArray.map(field => {
      val dataType = getDataType(field.getValue)
      StructField(field.getKey, dataType = dataType, nullable = true)
    })
    new StructType(fields)
  }

  /**
   * Convert the Json Array to ArrayType
   *
   * @param iterator - iterator
   * @return
   */
  private def extractArraySchema(iterator: Iterator[java.util.Map.Entry[String, JsonNode]]): ArrayType = {
    val da = iterator.toArray.toList
    println(da)
    val fields = da.map(field => {
      val dataType = getDataType(field.getValue)
      StructField(field.getKey, dataType = dataType, nullable = true)
    })
    new ArrayType(fields.head.dataType, containsNull = true)
  }
}

private[hl7] object SchemaHelper extends SchemaHelper