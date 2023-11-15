package Abgabe2

import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.collection.convert.ImplicitConversions.`set asScala`
import scala.io.Source
import scala.jdk.CollectionConverters.MapHasAsJava

object GenerateData {
  def apply(jedis:Jedis, pipeline: Pipeline): GenerateData = new GenerateData(jedis, pipeline)
}

class GenerateData(jedis: Jedis, pipeline: Pipeline) {
  private val matches = collection.immutable.Map.newBuilder[String, Match]

  private case class Match(index: Int, date: String, homeTeam: String, awayTeam: String)

  def generate(): Unit = {
    //generateResults(pipeline)
    //generateGoals(pipeline)
    generateShootouts(pipeline)
  }

  private def generateResults(pipeline: Pipeline): Unit = {
    val results = Source.fromFile("./results.csv")
    val iterator = results.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val resultsHashKey = s"results:" + index
      val fields = row.split(",")

      pipeline.lpush("games", s"${fields(0)}:${fields(1)}:${fields(2)}")
      pipeline.lset("games", index, s"${fields(0)}:${fields(1)}:${fields(2)}")

      pipeline.sadd(s"date:${fields(0)}", resultsHashKey)
      pipeline.sadd(s"home:${fields(1)}", resultsHashKey)
      pipeline.sadd(s"away:${fields(2)}", resultsHashKey)
//      pipeline.lset("games", index, s"${fields(0)}:${fields(1)}:${fields(2)}")

      val table = Map(
        "date" -> fields(0),
        "home_team_id" -> fields(1),
        "away_team_id" -> fields(2),
        "home_score" -> fields(3),
        "away_score" -> fields(4)
      )

      pipeline.hset(resultsHashKey, table.asJava)
      pipeline.sync()
    }
    println("Results - Done")
  }

  private def generateGoals(pipeline: Pipeline): Unit = {
    val goals = Source.fromFile("./goalscorers.csv")
    val iterator = goals.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val goalHashKey = s"goal:" + index
      val fields = row.split(",")


      val match_id = jedis.sinter(s"date:${fields(0)}", s"home:${fields(1)}", s"away:${fields(2)}").headOption.getOrElse("")
      /*val id = jedis.lpos("games", s"${fields(0)}:${fields(1)}:${fields(2)}")
      val match_id = s"results:$id"*/

      val table = Map(
        "results_id" -> match_id,
        "team_id" -> s"team:${fields(3)}",
        "scorer_id" -> s"scorer:${fields(4)}",
        "minute" -> fields(5),
        "own_goal" -> fields(6),
        "penalty" -> fields(7),
      )

      pipeline.hset(goalHashKey, table.asJava)
      pipeline.sadd("team_set", s"team:${fields(3)}", s"scorer:${fields(4)}")

      val resultsKey = match_id
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
    val goals = Source.fromFile("./shootouts.csv")
    val iterator = goals.getLines().drop(1).zipWithIndex

    for ((row, index) <- iterator) {
      val goalHashKey = s"shootout:" + index
      val fields = row.split(",")

      val match_id = jedis.sinter(s"date:${fields(0)}", s"home:${fields(1)}", s"away:${fields(2)}").headOption

      if (match_id.nonEmpty)
        pipeline.hset(goalHashKey, "results_id", match_id.get)

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
