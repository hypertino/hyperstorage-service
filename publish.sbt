// Sonatype repositary publish options
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false
pomIncludeRepository := { _ => false}

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
    </developers>

// Sonatype credentials
credentials ++= (for {
  username <- Option(System.getenv().get("sonatype_username"))
  password <- Option(System.getenv().get("sonatype_password"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq

// pgp keys and credentials
pgpSecretRing := file("./travis/ht-oss-private.asc")
pgpPublicRing := file("./travis/ht-oss-public.asc")
usePgpKeyHex("F8CDEF49B0EDEDCC")
pgpPassphrase := Option(System.getenv().get("oss_gpg_passphrase")).map(_.toCharArray)
