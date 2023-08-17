import sbt.Keys._
import sbt._

object ProjectSettings {
  lazy val root = commonSettings ++ Dependencies.hl7parser

  private lazy val general = Seq(
    version := version.value,
    scalaVersion := Versions.Scala11,
    organization := "com",
    organizationName := "hl7parser",
    developers := List(Developer("srinivasan",
      "Srinivasan Ramalingam",
      "eceseenu19898.3@gmail.com",
      new URL("http://google.com"))),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    cancelable in Global := true
  )

  private lazy val commonSettings = general ++ Assembly.settings
}
