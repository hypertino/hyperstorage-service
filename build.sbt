name := "hyper-storage"

organization := "com.hypertino"

version := "0.2-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

ramlHyperbusSources := Seq(
  ramlSource(
    path = "api/hyper-storage-service-api/hyperstorage.raml",
    packageName = "com.hypertino.hyperstorage.api",
    isResource = false
  )
)

buildInfoPackage := "com.hypertino.hyperstorage"

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)


// BuildInfo
lazy val root = (project in file(".")).enablePlugins(BuildInfoPlugin, Raml2Hyperbus)

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "com.hypertino"               %% "service-control"              % "0.3-SNAPSHOT",
  "com.hypertino"               %% "service-config"               % "0.2-SNAPSHOT",
  "com.hypertino"               %% "service-metrics"              % "0.3-SNAPSHOT",
  "com.hypertino"               %% "typesafe-config-binders"      % "0.13-SNAPSHOT",

  "com.hypertino"               %% "hyperbus"                     % "0.2-SNAPSHOT",
  "com.hypertino"               %% "hyperbus-t-inproc"            % "0.2-SNAPSHOT",
  "com.hypertino"               %% "hyperbus-t-kafka"             % "0.2-SNAPSHOT",
  "com.hypertino"               %% "hyperbus-t-zeromq"            % "0.2-SNAPSHOT",

  "com.hypertino"               %% "cassandra-binders"            % "0.2-SNAPSHOT",
  "com.hypertino"               %% "expression-parser"            % "0.1-SNAPSHOT",

  "com.typesafe.akka"           %% "akka-cluster"                 % "2.4.19",
  "com.typesafe.akka"           %% "akka-slf4j"                   % "2.4.19",

  "com.datastax.cassandra"      % "cassandra-driver-core"         % "2.1.9",
  "ch.qos.logback"              % "logback-classic"               % "1.1.3",
  "org.scalamock"               %% "scalamock-scalatest-support"  % "3.5.0"         % "test",
  "org.mockito"                 % "mockito-all"                   % "1.10.19"       % "test",
  "com.typesafe.akka"           %% "akka-testkit"                 % "2.4.19"         % "test",
  "org.cassandraunit"           % "cassandra-unit"                % "2.2.2.1"       % "test",
  "junit"                       % "junit"                         % "4.12"          % "test",
  "org.pegdown"                 % "pegdown"                       % "1.6.0"         % "test"
)

testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports", "-oDS")

parallelExecution in Test := false

fork in Test := true

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)
