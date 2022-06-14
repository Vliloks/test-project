name := "test-project"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3",
  "commons-io" % "commons-io" % "2.11.0",
  "org.postgresql" % "postgresql" % "42.3.3",
  "com.typesafe" % "config" % "1.3.3"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



