name := "hyperstorage"

organization := "com.hypertino"

version := "0.6.1-SNAPSHOT"

crossScalaVersions := Seq("2.12.3", "2.11.11")

scalaVersion := crossScalaVersions.value.head

ramlHyperbusSources := Seq(
  ramlSource(
    path = "api/hyperstorage-service-api/hyperstorage.raml",
    packageName = "com.hypertino.hyperstorage.api",
    isResource = false,
    baseClasses = Map(
      "ContentPut" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
      "ContentDelete" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
      "ContentPost" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
      "ContentPatch" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
//      "ViewPut" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
      "ViewDelete" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
      "HyperStorageTransaction" → Seq("com.hypertino.hyperstorage.workers.primary.HyperStorageTransactionBase"),
      "HyperStorageTransactionCreated" → Seq("com.hypertino.hyperstorage.workers.primary.HyperStorageTransactionBase")
    )
  ),
  ramlSource(
      path = "api/internal/internal-api.raml",
      packageName = "com.hypertino.hyperstorage.internal.api",
      isResource = false,
      baseClasses = Map(
        "RemoteTask" → Seq("com.hypertino.hyperstorage.sharding.ShardTask")
      )
  )
)

buildInfoPackage := "com.hypertino.hyperstorage"

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)


// BuildInfo
lazy val root = (project in file(".")).enablePlugins(BuildInfoPlugin, Raml2Hyperbus)

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "com.hypertino"               %% "service-control"              % "0.3.0",
  "com.hypertino"               %% "service-config"               % "0.2.0",
  "com.hypertino"               %% "service-metrics"              % "0.3.0",
  "com.hypertino"               %% "typesafe-config-binders"      % "0.2.0",

  "com.hypertino"               %% "hyperbus"                     % "0.4-SNAPSHOT",
//  "com.hypertino"               %% "hyperbus-t-kafka"             % "0.2-SNAPSHOT",
//  "com.hypertino"               %% "hyperbus-t-zeromq"            % "0.2-SNAPSHOT",

  "com.hypertino"               %% "cassandra-binders"            % "0.3.0",
  "com.hypertino"               %% "expression-parser"            % "0.2.1",

  "com.typesafe.akka"           %% "akka-cluster"                 % "2.4.20",
  "com.typesafe.akka"           %% "akka-slf4j"                   % "2.4.20",

  "com.datastax.cassandra"      % "cassandra-driver-core"         % "2.1.10.3",
  "ch.qos.logback"              % "logback-classic"               % "1.2.3",
  "com.hypertino"               %% "hyperbus-t-inproc"            % "0.3-SNAPSHOT"  % "test",
  "org.scalamock"               %% "scalamock-scalatest-support"  % "3.5.0"         % "test",
  "org.mockito"                 % "mockito-all"                   % "1.10.19"       % "test",
  "com.typesafe.akka"           %% "akka-testkit"                 % "2.4.20"        % "test",
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
