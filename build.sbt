crossScalaVersions := Seq("2.12.4", "2.11.12")

scalaVersion in Global := crossScalaVersions.value.head

lazy val commonSettings = Seq(
  version := "0.7.2-SNAPSHOT",
  organization := "com.hypertino",
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public")
  ),
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full),
  parallelExecution in Test := false,
  fork := true,
  logBuffered in Test := false
)

lazy val hyperstorage = project in file("hyperstorage") enablePlugins(BuildInfoPlugin, Raml2Hyperbus) settings (
  commonSettings,
  publishSettings,
  name := "hyperstorage",
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
        "ViewDelete" → Seq("com.hypertino.hyperstorage.workers.primary.PrimaryWorkerRequest"),
        "HyperStorageTransaction" → Seq("com.hypertino.hyperstorage.workers.primary.HyperStorageTransactionBase"),
        "HyperStorageTransactionCreated" → Seq("com.hypertino.hyperstorage.workers.primary.HyperStorageTransactionBase")
      )
    ),
    ramlSource(
        path = "hyperstorage/internal-api/internal-api.raml",
        packageName = "com.hypertino.hyperstorage.internal.api",
        isResource = false,
        baseClasses = Map(
          "TasksPost" → Seq("com.hypertino.hyperstorage.sharding.RemoteTaskBase")
        )
    )
  ),
  buildInfoPackage := "com.hypertino.hyperstorage",
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
  libraryDependencies ++= Seq(
    "com.hypertino"               %% "service-control"              % "0.4.1",
    "com.hypertino"               %% "service-config"               % "0.2.3",
    "com.hypertino"               %% "service-metrics"              % "0.3.1",
    "com.hypertino"               %% "typesafe-config-binders"      % "0.2.0",

    "com.hypertino"               %% "hyperbus"                     % "0.6-SNAPSHOT",
  //  "com.hypertino"               %% "hyperbus-t-kafka"             % "0.2-SNAPSHOT",
    "com.hypertino"               %% "hyperbus-t-inproc"            % "0.6-SNAPSHOT",
    "com.hypertino"               %% "hyperbus-t-zeromq"            % "0.6-SNAPSHOT",
    "com.hypertino"               %% "hyperbus-consul-resolver"     % "0.3-SNAPSHOT",

    "com.hypertino"               %% "cassandra-binders"            % "0.5-SNAPSHOT",
    "com.hypertino"               %% "expression-parser"            % "0.2.1",

    "com.typesafe.akka"           %% "akka-cluster"                 % "2.4.20",
    "com.typesafe.akka"           %% "akka-slf4j"                   % "2.4.20",

    "ch.qos.logback"              % "logback-classic"               % "1.2.3",
    "com.hypertino"               %% "hyperbus-t-inproc"            % "0.6-SNAPSHOT"  % "test",
    "org.scalamock"               %% "scalamock-scalatest-support"  % "3.5.0"         % "test",
    "org.mockito"                 % "mockito-all"                   % "1.10.19"       % "test",
    "com.typesafe.akka"           %% "akka-testkit"                 % "2.4.20"        % "test",
    //"org.cassandraunit"           % "cassandra-unit"                % "2.2.2.1"       % "test",
    "org.cassandraunit"           % "cassandra-unit"                % "3.3.0.2"       % "test",
    "junit"                       % "junit"                         % "4.12"          % "test",
    "org.pegdown"                 % "pegdown"                       % "1.6.0"         % "test"
  ),
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports", "-oDSI")
)

lazy val perftest = project in file("perftest") enablePlugins(Raml2Hyperbus) settings (
  commonSettings,
  ramlHyperbusSources := Seq(
  ramlSource(
      path = "api/hyperstorage-service-api/hyperstorage.raml",
      packageName = "com.hypertino.hyperstorage.api",
      isResource = false
    )
  ),
  libraryDependencies ++= Seq(
    "com.hypertino"               %% "hyperbus"                     % "0.6-SNAPSHOT",
    "com.hypertino"               %% "hyperbus-t-zeromq"            % "0.6-SNAPSHOT",
    "com.hypertino"               %% "hyperbus-t-inproc"            % "0.6-SNAPSHOT",
    "ch.qos.logback"              % "logback-classic"               % "1.2.3",
    "com.storm-enroute"           %% "scalameter"                   % "0.8.2",
    "com.hypertino"               %% "service-config"               % "0.2.0",
    "com.hypertino"               %% "hyperbus-consul-resolver"     % "0.3-SNAPSHOT"
  )
)

lazy val `hyperstorage-root` = project.in(file(".")) enablePlugins(Raml2Hyperbus) settings (
  publishSettings,
  publishArtifact := false,
  publishArtifact in Test := false,
  publish := {},
  publishLocal := {},
  ramlHyperbusSources := Seq.empty
) aggregate (
  hyperstorage,
  perftest
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false},
  pomExtra :=
    <url>https://github.com/hypertino/hyperstorage-service</url>
    <licenses>
      <license>
        <name>BSD-style</name>
        <url>http://opensource.org/licenses/BSD-3-Clause</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:hypertino/hyperstorage-service.git</url>
      <connection>scm:git:git@github.com:hypertino/hyperstorage-service.git</connection>
    </scm>
    <developers>
      <developer>
        <id>maqdev</id>
        <name>Magomed Abdurakhmanov</name>
        <url>https://github.com/maqdev</url>
      </developer>
      <developer>
        <id>hypertino</id>
        <name>Hypertino</name>
        <url>https://github.com/hypertino</url>
      </developer>
    </developers>,

  // pgp keys and credentials
  pgpSecretRing := file("./travis/script/ht-oss-private.asc"),
  pgpPublicRing := file("./travis/script/ht-oss-public.asc"),
  usePgpKeyHex("F8CDEF49B0EDEDCC"),
  pgpPassphrase := Option(System.getenv().get("oss_gpg_passphrase")).map(_.toCharArray)
)

// Sonatype credentials
credentials ++= (for {
  username <- Option(System.getenv().get("sonatype_username"))
  password <- Option(System.getenv().get("sonatype_password"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq
