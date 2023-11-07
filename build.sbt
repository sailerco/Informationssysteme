ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "h2_Abgabe"
  )

libraryDependencies += "com.typesafe.slick" %% "slick-codegen" % "3.5.0-M4"

libraryDependencies ++= Seq(
  "com.h2database" % "h2" % "2.2.224",
  "redis.clients" % "jedis" % "5.0.2",
  "com.typesafe.slick" %% "slick" % "3.5.0-M4",
  "org.slf4j" % "slf4j-nop" % "2.0.5",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.5.0-M4"
)




lazy val slick = taskKey[Seq[File]]("Generate Tables.scala")
slick := {
  val dir = (Compile / sourceManaged).value
  val outputDir = dir / "slick"
  val url = "jdbc:h2:/localhost/~/DB-IS" // connection info
  val jdbcDriver = "org.h2.Driver"
  val slickDriver = "slick.jdbc.H2Profile"
  val pkg = "demo"
  val password = "123"
  val user = "sa"

  val cp = (Compile / dependencyClasspath).value
  val s = streams.value

  runner.value.run("slick.codegen.SourceCodeGenerator",
    cp.files,
    Array(slickDriver, jdbcDriver, url, outputDir.getPath, pkg, user, password),
    s.log).failed foreach (sys error _.getMessage)

  val file = outputDir / pkg / "Tables.scala"

  Seq(file)
}