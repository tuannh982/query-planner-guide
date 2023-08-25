lazy val root = (project in file("."))
  .aggregate(
    `utils`,
    `core`,
    `demo`
  )
  .settings(
    name := "root",
    publish / skip := true,
    organization := ProjectInfo.organization,
    version := ProjectInfo.buildVersion,
    scalaVersion := ProjectInfo.scalaVersion
  )

lazy val `utils` = project
lazy val `core`  = project.dependsOn(`utils`)
lazy val `demo`  = project.dependsOn(`core`)
