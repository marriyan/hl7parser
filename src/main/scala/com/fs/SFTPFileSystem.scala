package com.fs

import com.cleanly
import com.fs.domain.SFTPSourceConfig
import org.apache.commons.vfs2.provider.sftp.{IdentityInfo, SftpFileSystemConfigBuilder}
import org.apache.commons.vfs2.{FileObject, FileSystemOptions, FileType, VFS}

import java.io.{File, InputStream, OutputStream}
import java.net.URLEncoder
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class SFTPFileSystem(config: SFTPSourceConfig)
  extends FileSystem(config) {

  lazy private val sftpUrl =
    s"""sftp://${URLEncoder.encode(config.username, charset.name)}:${URLEncoder.encode(config.password, charset.name)}
       |@${URLEncoder.encode(config.host, charset.name)}:${URLEncoder.encode(config.port, charset.name)}
       |""".stripMargin.replaceAll("\n", "")
  lazy private val fsManager = VFS.getManager


  private def getIdentityInfo: Try[IdentityInfo] = Try {
    val inputStream = getClass.getResourceAsStream("/privatekey.pem")
    val tempFile = File.createTempFile("privatekey", ".pem")
    tempFile.deleteOnExit()
    Files.copy(inputStream, Paths.get(tempFile.getAbsolutePath),
      StandardCopyOption.REPLACE_EXISTING)
    new IdentityInfo(tempFile)
  }

  private def createDefaultOptions: FileSystemOptions = {
    val options = new FileSystemOptions()
    SftpFileSystemConfigBuilder.getInstance()
      .setSessionTimeoutMillis(options, 10000)
    SftpFileSystemConfigBuilder.getInstance()
      .setStrictHostKeyChecking(options, "no")
    SftpFileSystemConfigBuilder.getInstance
      .setPreferredAuthentications(options, "password")
    getIdentityInfo match {
      case Failure(exception) => throw exception
      case Success(identityInfo) => SftpFileSystemConfigBuilder
        .getInstance.setIdentityProvider(options,
        identityInfo)
    }
    options
  }

  private def safelyClose[T](path: String)(block: FileObject => T): T = {
    val sftpFileUrl = s"$sftpUrl$path"
    cleanly(fsManager.resolveFile(sftpFileUrl,
      createDefaultOptions))(_.close()) {
      fsObject => block(fsObject)
    }.get
  }

  /**
   * read the file from Source & convert into Stream
   *
   * @param path - path
   * @return
   */
  override def openInputStream(path: String): InputStream = {
    val sftpFileUrl = s"$sftpUrl$path"
    fsManager.resolveFile(sftpFileUrl, createDefaultOptions).getContent.getInputStream
  }


  /**
   * write data into source
   *
   * @param path - path
   * @return
   */
  override def openOutputStream(path: String): OutputStream = {
    val sftpFileUrl = s"$sftpUrl$path"
    fsManager.resolveFile(sftpFileUrl, createDefaultOptions).getContent.getOutputStream
  }

  /**
   * Check File exist Condition
   *
   * @param path -path
   * @return
   */
  override def fileExists(path: String): Boolean =
    safelyClose(path) {
      fsObject => fsObject.exists()
    }

  /**
   * Delete the File in Source
   *
   * @param path
   * @return
   */
  override def deleteFile(path: String): Boolean = safelyClose(path) {
    fsObject => fsObject.delete()
  }

  /**
   * Create the new Directory in SourceFilesystem
   *
   * @param path
   * @return
   */
  override def createDirectory(path: String): Unit = safelyClose(path) {
    fsObject => fsObject.createFolder()
  }

  /**
   * check path is File
   *
   * @param path -path
   * @return
   */
  override def isFile(path: String): Boolean = safelyClose(path) {
    fsObject => fsObject.exists() && fsObject.isFile
  }

  override def listFiles(path: String, filter: String => Boolean = _ => true): List[String] = safelyClose(path) {
    fsObject =>
      if (fsObject.getType == FileType.FOLDER) {
        return listFilesInternal(fsObject, filter)
      }
      throw new IllegalArgumentException(s"Path($path) is not Folder.")
  }

  private def listFilesInternal(folder: FileObject,
                                filter: String => Boolean): List[String] = {
    val fileList = ListBuffer.empty[String]

    def listFilesHelper(files: List[FileObject]): Unit = {
      if (files.nonEmpty) {
        val currentFile = files.head

        if (currentFile.getType == FileType.FILE) {
          if (filter(currentFile.getName.getPath))
            fileList += currentFile.getName.getPath
        } else if (currentFile.getType == FileType.FOLDER) {
          val children = currentFile.getChildren.toList
          listFilesHelper(files.tail ++ children)
        }
        listFilesHelper(files.tail)
      }
    }

    val files = folder.getChildren.toList
    listFilesHelper(files)
    fileList.toList
  }

  /**
   * check file or folder exist.
   *
   * @param path
   * @return
   */
  override def isExist(path: String): Boolean = safelyClose(path) {
    fsObject => fsObject.exists()
  }
}
