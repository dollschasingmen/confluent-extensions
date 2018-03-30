import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbt.{Defaults, _}
import sbtassembly.AssemblyKeys._
import sbtassembly._

import scalariform.formatter.preferences.{AlignSingleLineCaseStatements, DoubleIndentClassDeclaration}

object BuildSettings {

  lazy val basicSettings = Seq[Setting[_]](
    organization := "com.github.dollschasingmen.confluent.extensions",
    description := "Confluent Extensions",
    scalaVersion := "2.11.8",
    resolvers ++= Dependencies.resolutionRepos,
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
  )

  lazy val scalariformSettings = SbtScalariform.scalariformSettings ++ Seq[Setting[_]](
    ScalariformKeys.preferences := ScalariformKeys.preferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
  )

  lazy val buildSettings = basicSettings ++ scalariformSettings ++ Defaults.coreDefaultSettings

  lazy val assemblySettings = Seq[Setting[_]](
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
