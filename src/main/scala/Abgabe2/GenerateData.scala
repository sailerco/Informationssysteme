package Abgabe2

import redis.clients.jedis.Pipeline

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

  private def generateResults(pipeline: Pipeline): Unit = {
    val results = Source.fromFile("D:/Informationssysteme/results.csv")
    val iterator = results.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val resultsHashKey = s"results:" + index
      val fields = row.split(",")

      val matchData = Match(index, fields(0), fields(1), fields(2))
      val index_mapping = s"${fields(0)}_${fields(1)}_${fields(2)}"
      matches += (index_mapping -> matchData)

      val table = Map(
        "date" -> fields(0),
        "home_team_id" -> s"team:${fields(1)}",
        "away_team_id" -> s"team:${fields(2)}",
        "home_score" -> fields(3),
        "away_score" -> fields(4),
      )

      pipeline.hset(resultsHashKey, table.asJava)
      pipeline.sadd("team_set", s"team:${fields(1)}", s"team:${fields(2)}")
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

      val table = Map(
        "results_id" -> match_id.get.toString,
        "team_id" -> s"team:${fields(3)}",
        "scorer_id" -> s"scorer:${fields(4)}",
        "minute" -> fields(5),
        "own_goal" -> fields(6),
        "penalty" -> fields(7),
      )
      pipeline.hset(goalHashKey, table.asJava)
      pipeline.sadd("team_set", s"team:${fields(3)}", s"scorer:${fields(4)}")

      val resultsKey = s"results:${match_id.get}"
      val homeGoalsInResults = pipeline.hget(resultsKey, "home_goals")
      val awayGoalsInResults = pipeline.hget(resultsKey, "away_goals")
      pipeline.sync()

      if (fields(3) == fields(1)) {
        updateGoals(pipeline, resultsKey, homeGoalsInResults.get(), "home_goals", index)
      } else if (fields(3) == fields(2)) {
        updateGoals(pipeline, resultsKey, awayGoalsInResults.get(), "away_goals", index)
      }
      pipeline.sync()
    }
    println("Goalscorers - Done")
    goals.close()
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

  private def updateGoals(pipeline: Pipeline, matchId: String, currentGoals: String, goalType: String, index: Int): Unit = {
    val updatedGoals = Option(currentGoals).fold(index.toString)(goals =>
      if(!goals.split(";").contains(index.toString))
        s"$goals;$index"
      else
        goals
    )
    pipeline.hset(matchId, goalType, updatedGoals)
  }
}
