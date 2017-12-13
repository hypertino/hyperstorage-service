name := "hyperstorage"

organization := "com.hypertino"

version := "0.7.1-SNAPSHOT"

crossScalaVersions := Seq("2.12.4", "2.11.12")

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
        "TasksPost" → Seq("com.hypertino.hyperstorage.sharding.RemoteTaskBase")
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

  "com.hypertino"               %% "hyperbus"                     % "0.5.1-SNAPSHOT",
//  "com.hypertino"               %% "hyperbus-t-kafka"             % "0.2-SNAPSHOT",
  "com.hypertino"               %% "hyperbus-t-zeromq"            % "0.4-SNAPSHOT",
  "com.hypertino"               %% "hyperbus-consul-resolver"     % "0.3-SNAPSHOT",

  "com.hypertino"               %% "cassandra-binders"            % "0.5-SNAPSHOT",
  "com.hypertino"               %% "expression-parser"            % "0.2.1",

  "com.typesafe.akka"           %% "akka-cluster"                 % "2.4.20",
  "com.typesafe.akka"           %% "akka-slf4j"                   % "2.4.20",

  "ch.qos.logback"              % "logback-classic"               % "1.2.3",
  "com.hypertino"               %% "hyperbus-t-inproc"            % "0.5.1-SNAPSHOT"  % "test",
  "org.scalamock"               %% "scalamock-scalatest-support"  % "3.5.0"         % "test",
  "org.mockito"                 % "mockito-all"                   % "1.10.19"       % "test",
  "com.typesafe.akka"           %% "akka-testkit"                 % "2.4.20"        % "test",
  //"org.cassandraunit"           % "cassandra-unit"                % "2.2.2.1"       % "test",
  "org.cassandraunit"           % "cassandra-unit"                % "3.3.0.2"       % "test",
  "junit"                       % "junit"                         % "4.12"          % "test",
  "org.pegdown"                 % "pegdown"                       % "1.6.0"         % "test"
)

//dependencyOverrides ++= Set(
//  "com.google.guava"            % "guava"                         % "19.0"          % "test"
//)

testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports", "-oDSI")

parallelExecution in Test := false

fork in Test := true
logBuffered in Test := false

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)
