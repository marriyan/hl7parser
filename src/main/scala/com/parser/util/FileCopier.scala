package com.parser.util

import com.cleanly
import com.fs.FileSystem
import org.apache.commons.io.IOUtils

import java.io.File
import java.nio.file.Paths
import scala.util.Try

class FileCopier(source: FileSystem[_],
                 destination: FileSystem[_]) {

  private def copyFileInternal(sourcePath: String,
                               destinationPath: String): Try[Unit] = {
    cleanly((source.openInputStream(sourcePath),
      destination.openOutputStream(destinationPath)))(fs => {
      fs._2.close()
      fs._1.close()
    }) {
      fs => IOUtils.copyLarge(fs._1, fs._2)
    }
  }

  def copy(sourcePath: String,
           destinationPath: String,
           filter: String => Boolean = _ => true): String = {
    val srcPath = new File(sourcePath)
    if (source.isFile(sourcePath)) {
      val desPath=Paths.get(destinationPath, srcPath.getName).toString
      copyFileInternal(sourcePath, desPath)
      desPath
    }
    else if (source.isExist(sourcePath)
      && !source.isFile(sourcePath)) {
      val desFolder = srcPath.getParent
      source.listFiles(sourcePath, filter).map {
        m =>
          val destinationFile = if (desFolder.length > 1)
            m.replace(desFolder, destinationPath)
          else
            s"$destinationPath$m"
          (m, destinationFile)
      }.foreach {
        f => copyFileInternal(f._1, f._2)
      }
      Paths.get(destinationPath, srcPath.getName).toString
    }
    else {
      throw new RuntimeException(s"Source file($sourcePath) does not exist.")
    }
  }
}