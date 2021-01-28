package com.knoldus.CypherSpark

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.value.CypherValue
import org.slf4j.LoggerFactory
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.impl.MorpheusConverters._

import scala.util.parsing.json.JSONObject


object MorpheusScala_MongoDB {

    @transient private val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {


    LOGGER.info("Creating Spark Session")
    var COLLECTION: String = "collection"
    var COLLECTION_NAME: String = "movie_graphs"
    var MONGO_INPUT_CONF: String = "spark.mongodb.input.uri"
    var MONGO_OUTPUT_CONF: String = "spark.mongodb.output.uri"
    var MONGO_URI: String = "mongodb://127.0.0.1/"
    //var path: String =  "/data/MODELOS/Customer-Segmentation-Unsupervised-ML-Model/OrdersAnalysisTask.csv"

    val spark = SparkSession
      .builder()
      .config("spark.master","local[*]")
      .config(MONGO_INPUT_CONF, MONGO_URI +"org."+ COLLECTION_NAME)
      .config(MONGO_OUTPUT_CONF, MONGO_URI +"org."+ COLLECTION_NAME)
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    val sc = spark.sparkContext

    LOGGER.info("==>> Create object to write in MONGODB")
    val writeConfig = WriteConfig(Map(COLLECTION -> COLLECTION_NAME, "writeConcern.w" -> "majority"),
        Some(WriteConfig(sc)))

    LOGGER.info("Creating Morpheus session")
    implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)

    LOGGER.debug("Reading csv files into data frames")
    val moviesDF = MovieActorDataFrames.createNode(spark, "movies.csv")
    val personsDF = MovieActorDataFrames.createNode(spark, "persons.csv")
    val actedInDF = MovieActorDataFrames.createNode(spark, "acted_in.csv")

    LOGGER.info("Creating element mapping for movies node")
    val movieNodeMapping = NodeMappingBuilder
      .withSourceIdKey("id:Int")
      .withImpliedLabel("Movies")
      .withPropertyKey(propertyKey = "title", sourcePropertyKey = "title")
      .withPropertyKey(propertyKey = "tagline", sourcePropertyKey = "tagline")
      .withPropertyKey(propertyKey = "summary", sourcePropertyKey = "summary")
      .withPropertyKey(propertyKey = "poster_image", sourcePropertyKey = "poster_image")
      .withPropertyKey(propertyKey = "duration", sourcePropertyKey = "duration")
      .withPropertyKey(propertyKey = "rated", sourcePropertyKey = "rated")
      .build

    LOGGER.info("Creating element mapping for person node")
    val personNodeMapping = NodeMappingBuilder
      .withSourceIdKey("id:Int")
      .withImpliedLabel("Person")
      .withPropertyKey("name", "name")
      .withPropertyKey("born", "born")
      .withPropertyKey("poster_image", "poster_image")
      .build

    LOGGER.info("Creating element mapping for the edge between two nodes")
    val actedInRelationMapping = RelationshipMappingBuilder
        .withSourceIdKey("rel_id:Int")
        .withSourceStartNodeKey("START_ID")
        .withSourceEndNodeKey("END_ID")
        .withRelType("ACTED_IN")
        .withPropertyKey("role", "role")
        .build

    LOGGER.info("Creating nodes and edges using mapping")
    val moviesNode = MorpheusElementTable.create(movieNodeMapping, moviesDF)
    val personsNode = MorpheusElementTable.create(personNodeMapping, personsDF)
    val actedInRelation = MorpheusElementTable.create(actedInRelationMapping, actedInDF)

    LOGGER.info("Creating Property Graph")
    val actorMovieGraph = morpheus.readFrom(personsNode,moviesNode,actedInRelation)


    LOGGER.info("Query Property Graph for the names of the actor nodes")
    val actors = actorMovieGraph.cypher(
        "MATCH (p:Person) return p.name AS Actor_Name"
    )
    actors.records.show

    LOGGER.info("Query to get titles of all Movie nodes")
    val movies = actorMovieGraph.cypher(
        "MATCH (m:Movies) return m.title AS Movie_Titles"
    )
    movies.records.show

    LOGGER.info("Query to read all actors and their respective movies")
    val actor_movies = actorMovieGraph.cypher(
        "MATCH (p:Person)-[a:ACTED_IN]->(m:Movies) RETURN p.name AS ACTOR_NAME, m.title AS MOVIE_TITLE"
    )
    actor_movies.records.show

    LOGGER.info("Query to read movie belonging to a particular actor")
    val movie = actorMovieGraph.cypher(
        "MATCH (p:Person{name:'Gloria Foster'}) -[a:ACTED_IN]->(m:Movies) RETURN m.title AS MOVIE_TITLE"
    )
    movie.records.show

    LOGGER.info("Query with Parameter Substitution")
    val param =  CypherValue.CypherMap(("movie_name", "The Matrix Revolutions"))
    val actorName = actorMovieGraph.cypher(
        s"MATCH (m:Movies{title:{movie_name}})<-[a:ACTED_IN]-(p:Person) RETURN p.name AS ACTOR_NAME",
        param)
    actorName.records.show

    LOGGER.info("All nodes!")
    val allNodes = actorMovieGraph.cypher(s"MATCH (GRAFO_NAME) RETURN GRAFO_NAME".stripMargin)
    allNodes.records.show


        allNodes.records.asMorpheus.df.toDF.createOrReplaceTempView("grafo")

        val query = spark.sql("SELECT * FROM grafo")

        LOGGER.info("|<---------------------------------------SAVE TO MONGO----------------------------------->|")

        val sparkDocuments = query.rdd.mapPartitions(partition => {

            val docs = partition.map(row => {

                var colMap: Map[String, String] = Map()

                row.schema.map(_.name).map(col => {
                    val colName = col.trim
                    val colIndex = row.fieldIndex(colName)
                    val colAnyVal = row.getAs[AnyVal](colIndex)

                    var colVal = if (colAnyVal == null) "NULL" else colAnyVal.toString()
                    if(colVal == null){
                        val nullCol = "NULL"
                        colVal = nullCol.asInstanceOf[String]
                    }
                    if (colVal.toString() == "NULL"){

                    } else {
                        colMap = colMap ++ Map(colName -> colVal)
                    }
                })

                val json = JSONObject(colMap).toString()

                val parseJson = Document.parse(json)

                parseJson
            }).toIterator
            docs
        })

        LOGGER.info("|<--------------------------------------------------------------------------------------->|")
        LOGGER.info("|<---------------------------------------SAVE TO MONGO----------------------------------->|")
        LOGGER.info("|<--------------------------------------------------------------------------------------->|")
        /** Query mongodb https://docs.mongodb.com/manual/tutorial/query-documents/ */
        MongoSpark.save(sparkDocuments, writeConfig)
        LOGGER.info("|<--------------------------------------------------------------------------------------->|")
        LOGGER.info("|<---------------------------------------SAVED IN MONGO---------------------------------->|")
        LOGGER.info("|<--------------------------------------------------------------------------------------->|")

    }
}