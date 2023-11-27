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
      val key = s"results:" + index
      val Array(date, homeTeam, awayTeam, homeScore, awayScore, tournament, city, country, neutral) = row.split(",")

      val matchData = Map(
        "date" -> date,
        "home_team" -> homeTeam,
        "away_team" -> awayTeam,
        "home_score" -> homeScore,
        "away_score" -> awayScore,
        "city" -> city,
        "country" -> country,
        "tournament" -> tournament,
        "neutral" -> neutral
      ).asJava

      pipeline.hset(key, matchData)
      pipeline.zadd("matches", index, s"$date:$homeTeam:$awayTeam")
      pipeline.sync()
    }
    println("Loading results is done")
    results.close()
  }

  private def generateGoals(): Unit = {
    val iterator = goals.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val pipeline: Pipeline = jedis.pipelined()
      val key = s"goal:" + index
      val Array(date, homeTeam, awayTeam, team, scorer, minute, ownGoal, penalty) = row.split(",")

      val matchID = jedis.zscore("matches", s"$date:$homeTeam:$awayTeam").toInt

      val goalData = Map(
        "results_id" -> matchID.toString,
        "team" -> team,
        "scorer" -> scorer,
        "minute" -> minute,
        "own_goal" -> ownGoal,
        "penalty" -> penalty
      ).asJava

      pipeline.hset(key, goalData)
      if (team == homeTeam) pipeline.zadd("home_goals", matchID, key)
      if (team == awayTeam) pipeline.zadd("away_goals", matchID, key)

      pipeline.sync()
    }
    println("Loading goalscorers is done")
    goals.close()
  }

  private def generateShootouts(): Unit = {
    val iterator = shootouts.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val key = s"shootout:" + index
      val Array(date, homeTeam, awayTeam, winner) = row.split(",")
      val matchID = jedis.zscore("matches", s"$date:$homeTeam:$awayTeam")

      if (matchID != null) {
        val pipeline: Pipeline = jedis.pipelined()
        pipeline.hset(key, "results_id", matchID.toInt.toString)
        pipeline.hset(key, "team", winner)
        pipeline.hset(s"results:${matchID.toInt}", "shootout_id", key)
        pipeline.sync()
      }
    }
    println("Loading shootouts is done")
    shootouts.close()
  }
}
