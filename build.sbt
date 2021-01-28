name := "SparkCypherDemo"

version := "0.1"

// Spark Information
val sparkVersion = "2.4.7"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "https://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at
  "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

  // spark-modules
  //"org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // open-cypher
  "org.opencypher" % "morpheus-spark-cypher" % "0.4.2",

  // spark packages
  //"graphframes" % "graphframes" % "0.4.0-spark2.1-s_2.11",

  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
  //"com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.1",

  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp-gpu
  //"com.johnsnowlabs.nlp" %% "spark-nlp-gpu" % "2.7.1",

  // MongoDB
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1",
  "org.mongodb" % "mongo-java-driver" % "3.11.2",
  "org.mongodb" % "bson" % "3.11.2",

  // testing
  //"org.scalatest" %% "scalatest" % "2.2.4" % "test",
  //"org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

  // nvidia rapids
  //"com.nvidia" %% "rapids-4-spark" % "0.1.0",
  //"ai.rapids" % "cudf" % "0.14",
)
