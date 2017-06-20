resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("com.hypertino" % "hyperbus-raml-sbt-plugin" % "0.2-SNAPSHOT")
