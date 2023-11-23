package Abgabe2

import redis.clients.jedis.{Jedis, Pipeline}

import scala.io.Source
import scala.jdk.CollectionConverters.MapHasAsJava

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
      val Array(date, home_team, away_team, home_score, away_score, tournament, city, country, neutral) = row.split(",")

      val table = Map(
        "date" -> date,
        "home_team" -> home_team,
        "away_team" -> away_team,
        "home_score" -> home_score,
        "away_score" -> away_score,
        "tournament" -> tournament,
        "city" -> city,
        "country" -> country,
        "neutral" -> neutral
      )

      pipeline.hset(resultsHashKey, table.asJava)
      pipeline.zadd("matches", index, s"$date:$home_team:$away_team")

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

      val Array(date, home_team, away_team, team, scorer, minute, own_goal, penalty) = row.split(",")

      val match_id = jedis.zscore("matches", s"$date:$home_team:$away_team")

      val table = Map(
        "results_id" -> match_id.toInt.toString,
        "team" -> team,
        "scorer" -> scorer,
        "minute" -> minute,
        "own_goal" -> own_goal,
        "penalty" -> penalty,
      )

      pipeline.hset(goalHashKey, table.asJava)

      if (team == home_team) pipeline.zadd("home_goals", match_id, goalHashKey)
      if (team == away_team) pipeline.zadd("away_goals", match_id, goalHashKey)

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
      val Array(date, home_team, away_team, winner) = row.split(",")

      val match_id = jedis.zscore("matches", s"$date:$home_team:$away_team")

      if (match_id != null) {
        pipeline.hset(shootHashKey, "results_id", match_id.toInt.toString)
        pipeline.hset(shootHashKey, "team", winner)
        pipeline.sync()
      }
    }
    println("Shootouts - Done")
    shootouts.close()
  }
}
