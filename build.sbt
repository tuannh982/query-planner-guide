lazy val root = (project in file("."))
  .aggregate(
    `utils`,
    `core`,
    `demo`
  )
  .settings(
    name := "root",
    publish / skip := true
  )

lazy val `utils` = project
lazy val `core` = project.dependsOn(`utils`)
lazy val `demo` = project.dependsOn(`core`)