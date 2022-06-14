name := "test-project"

version := "0.1"

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "commons-io" % "commons-io" % "2.11.0",
  "org.postgresql" % "postgresql" % "42.3.3",
)



