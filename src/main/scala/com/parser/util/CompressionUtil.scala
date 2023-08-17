package com.parser.util

import com.cleanly

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.io.Source
import scala.util.{Failure, Success}

object CompressionUtil extends App {
  /**
   * Compress the Input String
   *
   * @param input - Input String
   * @return
   */
  def compress(input: String): String = {
    val outputStream = new ByteArrayOutputStream()
    cleanly(new GZIPOutputStream(outputStream))(_.close()) {
      gzipOutputStream =>
        gzipOutputStream.write(input.getBytes("UTF-8"))
    } match {
      case Failure(exception) => throw exception
      case Success(_) =>
        Base64.getEncoder.encodeToString(outputStream.toByteArray)
    }
  }

  /**
   * Decompress the Input String
   *
   * @param compressedString - Compress String
   * @return
   */
  def decompress(compressedString: String): String = {
    val compressedBytes = Base64.getDecoder.decode(compressedString)
    val inputStream = new ByteArrayInputStream(compressedBytes)
    cleanly(new GZIPInputStream(inputStream))(_.close()) {
      gzipInputStream =>
        new String(Stream
          .continually(gzipInputStream.read(): Int)
          .takeWhile(_ != -1).map(_.toByte).toArray, "UTF-8")
    } match {
      case Failure(exception) => throw exception
      case Success(value) =>
        value
    }
  }

  val cString = compress(Source.fromFile("input/oru.hl7").mkString)
  println(cString)
  println(decompress(cString))
}
