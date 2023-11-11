package Abgabe2

import redis.clients.jedis.{Jedis, Pipeline}

import java.util.Map
import scala.io.Source
import scala.jdk.CollectionConverters.MapHasAsJava

object GenerateData {
  def apply(pipeline: Pipeline): GenerateData = new GenerateData(pipeline)
}

class GenerateData(pipeline: Pipeline) {
  private val matches = collection.immutable.Map.newBuilder[String, Match]
  private case class Match(index: Int, date: String, homeTeam: String, awayTeam: String)

  def generate(): Unit = {
    generateResults(pipeline)
    generateGoals(pipeline)
    generateShootouts(pipeline)
  }

  def generateResults(pipeline: Pipeline): Unit = {
    val results = Source.fromFile("D:/Informationssysteme/results.csv")
    val iterator = results.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val resultsHashKey = s"results:" + index
      val fields = row.split(",")

      val matchData = Match(index, fields(0), fields(1), fields(2))
      val index_mapping = s"${fields(0)}_${fields(1)}_${fields(2)}"
      matches += (index_mapping -> matchData)

      pipeline.hset(resultsHashKey, "date", fields(0))

      pipeline.sadd("team_set", s"team:${fields(1)}")
      pipeline.sadd("team_set", s"team:${fields(2)}")

      pipeline.hset(resultsHashKey, "home_team_id", s"team:${fields(1)}")
      pipeline.hset(resultsHashKey, "away_team_id", s"team:${fields(2)}")
      pipeline.hset(resultsHashKey, "home_score", fields(3))
      pipeline.hset(resultsHashKey, "away_score", fields(4))
      pipeline.sync()
    }
    println("Results - Done")
  }

  private def generateGoals(pipeline: Pipeline): Unit = {
    val goals = Source.fromFile("D:/Informationssysteme/goalscorers.csv")
    val iterator = goals.getLines().drop(1).zipWithIndex

    for ((row, index) <- iterator) {
      val goalHashKey = s"goal:" + index
      val fields = row.split(",")

      val index_mapping = s"${fields(0)}_${fields(1)}_${fields(2)}"
      val match_id = matches.result().get(index_mapping).map(_.index)
      pipeline.hset(goalHashKey, "results_id", match_id.get.toString)

      pipeline.sadd("team_set", s"team:${fields(3)}")
      pipeline.hset(goalHashKey, "team_id", s"team:${fields(3)}")

      pipeline.sadd("scorer_set", s"scorer:${fields(4)}")
      pipeline.hset(goalHashKey, "scorer_id", s"scorer:${fields(4)}")

      pipeline.hset(goalHashKey, "minute", fields(5))
      pipeline.hset(goalHashKey, "own_goal", fields(6))
      pipeline.hset(goalHashKey, "penalty", fields(7))

      pipeline.sync()
      val homeGoalsInResults = pipeline.hget("results:" + match_id.get, "home_goals")
      val awayGoalsInResults = pipeline.hget("results:" + match_id.get, "away_goals")
      pipeline.sync()
      if (fields(3) == fields(1)) {
        if (homeGoalsInResults.get() != null)
          pipeline.hset("results:" + match_id.get, "home_goals", homeGoalsInResults.get() + ";" + index)
        else
          pipeline.hset("results:" + match_id.get, "home_goals", index.toString)

      } else if (fields(3) == fields(2)) {
        if (awayGoalsInResults.get() != null)
          pipeline.hset("results:" + match_id.get, "away_goals", awayGoalsInResults.get() + ";" + index)
        else
          pipeline.hset("results:" + match_id.get, "away_goals", index.toString)
      }
      pipeline.sync()
    }
    println("Goalscorers - Done")
  }

  private def generateShootouts(pipeline: Pipeline): Unit = {
    val goals = Source.fromFile("D:/Informationssysteme/shootouts.csv")
    val iterator = goals.getLines().drop(1).zipWithIndex

    for ((row, index) <- iterator) {
      val goalHashKey = s"shootout:" + index
      val fields = row.split(",")

      val index_mapping = s"${fields(0)}_${fields(1)}_${fields(2)}"
      val match_id = matches.result().get(index_mapping).map(_.index)
      if (match_id.nonEmpty)
        pipeline.hset(goalHashKey, "results_id", match_id.get.toString)
      else
        pipeline.hset(goalHashKey, "results_id", "")

      pipeline.sadd("team_set", s"team:${fields(3)}")
      pipeline.hset(goalHashKey, "team_id", s"team:${fields(3)}")
      pipeline.sync()
    }
    println("Shootouts - Done")
  }
}
