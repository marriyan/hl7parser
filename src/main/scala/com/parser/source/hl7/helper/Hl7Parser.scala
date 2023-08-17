package com.parser.source.hl7.helper

import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.parser.{CanonicalModelClassFactory, DefaultXMLParser, PipeParser}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.xml.XmlMapper

import scala.util.Try

private[hl7] trait Hl7Parser extends Serializable {

  private def renameKeys(jsonNode: JsonNode): JsonNode = {
    val mapper = new ObjectMapper()
    jsonNode match {
      case objectNode: ObjectNode =>
        val updatedObjectNode = mapper.createObjectNode()
        val fields = objectNode.fields()

        while (fields.hasNext) {
          val field = fields.next()
          val originalKey = field.getKey
          val updatedKey = originalKey.trim
            .replaceAll(" ", "")
            .replace('.', '_')
          val updatedValue = renameKeys(field.getValue)
          updatedObjectNode.set(updatedKey, updatedValue)
        }
        updatedObjectNode
      case arrayNode: ArrayNode =>
        val updatedArrayNode = mapper.createArrayNode()
        val elements = arrayNode.elements()

        while (elements.hasNext) {
          val element = elements.next()
          val updatedElement = renameKeys(element)
          updatedArrayNode.add(updatedElement)
        }

        updatedArrayNode

      case _ =>
        jsonNode
    }
  }

  def parserHl7MessageToJson(version: String, hl7Message: String): String = {
    val hl7parser = new PipeParser(new CanonicalModelClassFactory(version))
    val message: Message = hl7parser.parse(hl7Message)
    val xmlParser = new DefaultXMLParser
    val xmlMapper = new XmlMapper()
    xmlMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    val jsonNode = renameKeys(xmlMapper.readTree(xmlParser.encode(message)))
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsString(jsonNode)
  }
}

private[hl7] object Hl7Parser extends Hl7Parser
