lazy val root = (project in file("."))
  .aggregate(
  )
  .settings(
    name := "root",
    publish / skip := true
  )

