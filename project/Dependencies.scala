import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "Confluent Platform" at "http://packages.confluent.io/maven/"
  )

  object V {
    val kafka = "1.0.0"
  }

  object Libraries {
    val connectApi             =  "org.apache.kafka"     % "connect-api"            % V.kafka
    val connectTransforms      =  "org.apache.kafka"     % "connect-transforms"     % V.kafka
  }
}
