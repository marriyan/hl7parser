import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyExcludedJars}
import sbtassembly.PathList

object Assembly {
  lazy val settings = Seq(
    assemblyJarName := s"${name.value.toLowerCase}_${version.value.toLowerCase}.jar",
    assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assemblyExcludedJars := {
      val dependencyJars = (Compile / dependencyClasspath).value
      val excludedJars = (assembly / fullClasspath).value filterNot  { jar =>
        dependencyJars.exists(dep => dep.data.getName == jar.data.getName)
      }
      excludedJars
    },
    assembly / assemblyOption := (assembly / assemblyOption)
      .value.withIncludeScala(includeScala = false)
  )
}
