package com.fs

import com.cleanly
import com.fs.domain.HdfsSourceConfig
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem => FsFileSystem}

import java.io.{InputStream, OutputStream}
import scala.collection.mutable.ListBuffer

class HdfsFileSystem(config: HdfsSourceConfig)
  extends FileSystem(config) {

  private def getFsFilesystem: FsFileSystem =
    FsFileSystem.get(config.conf)

  private def safelyClose[T](block: FsFileSystem => T): T = {
    cleanly(getFsFilesystem)(_.close)(block(_)).get
  }

  /**
   * read the file from Source & convert into Stream
   *
   * @param path - path
   * @return
   */
  override def openInputStream(path: String): InputStream =
    getFsFilesystem.open(new Path(path))

  /**
   * write data into source
   *
   * @param path - path
   * @return
   */
  override def openOutputStream(path: String): OutputStream =
    getFsFilesystem.create(new Path(path))

  /**
   * Check File exist Condition
   *
   * @param path -path
   * @return
   */
  override def fileExists(path: String): Boolean = safelyClose {
    fs =>
      val fsPath = new Path(path)
      fs.isFile(fsPath) && fs.exists(fsPath)
  }

  /**
   * Delete the File in Source
   *
   * @param path
   * @return
   */
  override def deleteFile(path: String): Boolean = safelyClose {
    fs => fs.delete(new Path(path), true)
  }

  /**
   * Create the new Directory in SourceFilesystem
   *
   * @param path
   * @return
   */
  override def createDirectory(path: String): Unit = safelyClose {
    fs => fs.mkdirs(new Path(path))
  }

  /**
   * check path is File
   *
   * @param path -path
   * @return
   */
  override def isFile(path: String): Boolean = safelyClose {
    fs => fs.isFile(new Path(path))
  }

  /**
   * List the files in the folders
   *
   * @param path
   * @return
   */
  override def listFiles(path: String,
                         filter: String => Boolean = _ => true): List[String] = safelyClose {
    fs =>
      val fileList = ListBuffer[String]()

      def traverseDirectory(path: Path): Unit = {
        val statusArray: Array[FileStatus] = fs.listStatus(path)
        for (status <- statusArray) {
          if (status.isDirectory) {
            traverseDirectory(status.getPath)
          } else {
            if (filter(status.getPath.toString))
              fileList += status.getPath.toString
          }
        }
      }

      traverseDirectory(new Path(path))
      fileList.toList
  }

  /**
   * check file or folder exist.
   *
   * @param path
   * @return
   */
  override def isExist(path: String): Boolean = safelyClose {
    fs => fs.exists(new Path(path))
  }

  def deleteOnExit(path: String): Boolean =
    getFsFilesystem.deleteOnExit(new Path(path))
}
