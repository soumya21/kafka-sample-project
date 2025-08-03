

ThisBuild / version := "0.1.0-SNAPSHOT"


ThisBuild / scalaVersion := "2.12.12"

val flinkVersion = "1.14.3"
val typeSafeCfg = "1.4.2"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-connector-kafka" % flinkVersion % "provided",
      "org.apache.flink" % "flink-core" % flinkVersion % "provided",
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % "3.0.1",
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
      "org.apache.kafka" %% "kafka" % "3.0.0",
      "com.typesafe" % "config" % typeSafeCfg,
      "org.json4s" %% "json4s-native" % "4.0.6"
    ),
    mainClass in Compile := Some("FlinkConsumerMain"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  ).settings(
    name := "FlinkKafkaApp"
  )

lazy val flinkApp = (project in file("flinkApp"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-connector-kafka" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
      "org.apache.flink" % "flink-core" % flinkVersion % "provided",
      "org.apache.kafka" %% "kafka" % "3.0.0",
      "com.typesafe" % "config" % typeSafeCfg,
      "org.json4s" %% "json4s-native" % "4.0.6"
    )

  )
// Path: project/assembly.sbt
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

