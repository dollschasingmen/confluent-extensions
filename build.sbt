import Dependencies.Libraries
import BuildSettings.{assemblySettings, buildSettings}

shellPrompt := { s => Project.extract(s).currentProject.id + " > " }

lazy val root = Project("confluent-extensions-root", file("."))
  .settings(
    buildSettings,
    publish := {},
    publishLocal := {}
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .aggregate(transforms)

lazy val transforms = Project("confluent-extensions-connect-transforms", file("transforms"))
  .settings(buildSettings: _*)
  .settings(assemblySettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Libraries.connectApi,
      Libraries.connectTransforms,

      // test
      Libraries.scalatest
    )
  )
