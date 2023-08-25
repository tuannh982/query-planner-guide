import sbt.*

object ProjectInfo {
  val organization = "io.github.tuannh982"
  val buildVersion = "0.0.1-SNAPSHOT"
  val scalaVersion = "2.12.17"
}

object Dependencies {
  val parserCombinator = "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
  val scalaTest        = "org.scalatest"          %% "scalatest"                % "3.2.15"
  val scalaMock        = "org.scalamock"          %% "scalamock"                % "5.2.0"
}
