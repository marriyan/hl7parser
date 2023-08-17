import Versions._
import sbt.Keys.libraryDependencies
import sbt._

object Dependencies {

  private lazy val hapi = Seq(
    "ca.uhn.hapi" % "hapi-base" % hapiVersion,
    "ca.uhn.hapi" % "hapi-structures-v25" % hapiVersion,
    "ca.uhn.hapi" % "hapi-structures-v23" % hapiVersion,
    "ca.uhn.hapi" % "hapi-structures-v231" % hapiVersion
  )

  private lazy val spark = Seq("org.apache.spark" %% "spark-sql" % sparkVersion % Provided)

  private lazy val common = Seq(
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.6.7",
    "org.apache.commons" % "commons-vfs2" % "2.4.1",
    "com.jcraft" % "jsch" % "0.1.55")

  private lazy val test = Seq(
    "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    "org.scalamock" %% "scalamock" % "4.4.0" % Test
  )


  private def depends( modules : ModuleID* ) : Seq[Def.Setting[Seq[ModuleID]]] = Seq(libraryDependencies ++= modules)

  lazy val hl7parser = depends(hapi : _*) ++ depends(spark : _*) ++ depends(common : _*) ++ depends(test : _*)
}
