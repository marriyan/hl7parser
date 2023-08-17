package com.fs

import com.fs.domain.SourceConfig

import java.io.{InputStream, OutputStream}
import java.nio.charset.Charset

private[com] abstract class FileSystem[C <: SourceConfig](config: C) {


  val charset: Charset = Charset.forName("UTF-8")

  /**
   * read the file from Source & convert into Stream
   *
   * @param path - path
   * @return
   */
  def openInputStream(path: String): InputStream

  /**
   * write data into source
   *
   * @param path - path
   * @return
   */
  def openOutputStream(path: String): OutputStream

  /**
   * Check File exist Condition
   *
   * @param path -path
   * @return
   */
  def fileExists(path: String): Boolean


  /**
   * Delete the File in Source
   *
   * @param path - path
   * @return
   */
  def deleteFile(path: String): Boolean

  /**
   * Create the new Directory in SourceFilesystem
   *
   * @param path - path
   * @return
   */
  def createDirectory(path: String): Unit

  /**
   * check path is File
   *
   * @param path -path
   * @return
   */
  def isFile(path: String): Boolean

  /**
   * check file or folder exist.
   *
   * @param path
   * @return
   */
  def isExist(path: String): Boolean

  /**
   * List the files in the folders
   *
   * @param path
   * @return
   */
  def listFiles(path: String,
                filter: String => Boolean = _ => true): List[String]
}
