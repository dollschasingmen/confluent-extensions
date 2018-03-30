resolvers += "Scala Style Checker" at "https://oss.sonatype.org/content/repositories/releases/org/scalastyle"

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
