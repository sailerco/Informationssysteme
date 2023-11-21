package Abgabe2

import redis.clients.jedis.{Jedis, Pipeline}

import scala.io.Source
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsJava}

object GenerateData {
  def apply(host: String, port: Int): GenerateData = new GenerateData(host, port)
}

class GenerateData(host: String, port: Int) {
  val jedis = new Jedis(host, port)
  private val results = Source.fromFile("./results.csv")
  private val goals = Source.fromFile("./goalscorers.csv")
  private val shootouts = Source.fromFile("./shootouts.csv")


  def generate(): Unit = {
    generateResults()
    generateGoals()
    generateShootouts()
  }

  private def generateResults(): Unit = {
    val iterator = results.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val pipeline: Pipeline = jedis.pipelined()
      val resultsHashKey = s"results:" + index
      val fields = row.split(",")

      val table = Map(
        "date" -> fields(0),
        "home_team" -> fields(1),
        "away_team" -> fields(2),
        "home_score" -> fields(3),
        "away_score" -> fields(4)
      )

      pipeline.hset(resultsHashKey, table.asJava)

      pipeline.sadd("result_ids", resultsHashKey)
      pipeline.sadd(s"date:${fields(0)}", index.toString)
      pipeline.sadd(s"home:${fields(1)}", index.toString)
      pipeline.sadd(s"away:${fields(2)}", index.toString)

      pipeline.sync()
    }
    println("Results - Done")
    results.close()
  }

  private def generateGoals(): Unit = {
    val iterator = goals.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val pipeline: Pipeline = jedis.pipelined()
      val goalHashKey = s"goal:" + index
      val fields = row.split(",")

      val match_id = getMatchID(s"date:${fields(0)}", s"home:${fields(1)}", s"away:${fields(2)}").next()

      val table = Map(
        "results_id" -> match_id,
        "team" -> fields(3),
        "scorer" -> fields(4),
        "minute" -> fields(5),
        "own_goal" -> fields(6),
        "penalty" -> fields(7),
      )

      pipeline.hset(goalHashKey, table.asJava)
      pipeline.sadd("goal_ids", goalHashKey)

      if (fields(3) == fields(1)) pipeline.zadd("home_goals", match_id.toDouble, goalHashKey)
      if (fields(3) == fields(2)) pipeline.zadd("away_goals", match_id.toDouble, goalHashKey)

      pipeline.sync()
    }
    println("Goalscorers - Done")
    goals.close()
  }

  private def generateShootouts(): Unit = {
    val iterator = shootouts.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val pipeline: Pipeline = jedis.pipelined()

      val shootHashKey = s"shootout:" + index
      val fields = row.split(",")

      val match_id = getMatchID(s"date:${fields(0)}", s"home:${fields(1)}", s"away:${fields(2)}")

      if (match_id.hasNext) {
        pipeline.hset(shootHashKey, "results_id", match_id.next())
        pipeline.hset(shootHashKey, "team", fields(3))
        pipeline.sync()
      }
    }
    println("Shootouts - Done")
    shootouts.close()
  }

  private def getMatchID(date: String, home: String, away: String): Iterator[String] = {
    jedis.sinter(date, home, away).iterator().asScala
  }
}
