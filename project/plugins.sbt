resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("com.hypertino" % "hyperbus-raml-sbt-plugin" % "0.5-SNAPSHOT")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
