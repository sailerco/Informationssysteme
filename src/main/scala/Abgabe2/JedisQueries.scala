package Abgabe2

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.{JedisPooled, Pipeline}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ListHasAsScala

object JedisQueries {
  def apply(host: String, port: Int): JedisQueries = new JedisQueries(host, port)
}

class JedisQueries(host: String, port: Int) extends SimpleQueries {
  val jedis = new JedisPooled(host, port)

  private val cursor = ScanParams.SCAN_POINTER_START
  private val results_keys: List[String] = scanOverHash(List(), cursor, new ScanParams().`match`("results:*").count(1000))
  private val goals_keys: List[String] = scanOverHash(List(), cursor, new ScanParams().`match`("goal:*").count(1000))

  override def close(): Future[Unit] = Future {
    jedis.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    //Create a set of all results_ids
    val resultsOfGoals = withPipeline { pipeline =>
      goals_keys.map(r => pipeline.hget(r, "results_id")).toSet
    }.map(_.get())

    // Retrieve scores and goals that happened in each match
    val groupedScores = withPipeline { pipeline =>
      resultsOfGoals
        .map(id => (pipeline.hget(s"results:$id", "home_score"),
          pipeline.hget(s"results:$id", "away_score"),
          pipeline.zrangeByScore("home_goals", id, id),
          pipeline.zrangeByScore("away_goals", id, id)
        ))
    }

    // Check if the recorded score matches the number of goals for each match
    groupedScores.forall {
      case (homeScore, awayScore, homeGoals, awayGoals) =>
        homeScore.get().toInt == homeGoals.get().toArray.length && awayScore.get().toInt == awayGoals.get().toArray.length
    }
  }

  override def countGoals(name: String): Future[Int] = Future {
    val scorer = withPipeline { pipeline =>
      goals_keys.map(key => pipeline.hget(key, "scorer"))
    }
    scorer.count(_.get() == name)
  }

  // Function to execute operations within a pipeline
  private def withPipeline[T](f: Pipeline => T): T = {
    val pipeline = jedis.pipelined()
    try {
      f(pipeline)
    } finally {
      pipeline.sync()
    }
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val scores = withPipeline { pipeline =>
      results_keys.map(key => (pipeline.hget(key, "home_score"), pipeline.hget(key, "away_score")))
    }
    scores.count { case (homeScore, awayScore) =>
      (min to max).contains(homeScore.get().toInt + awayScore.get().toInt)
    }
  }

  @tailrec
  private def scanOverHash(current: List[String], cursor: String, scanParams: ScanParams): List[String] = {
    val scanResults = jedis.scan(cursor, scanParams)
    val newCursor = scanResults.getCursor
    val updatedKeys = current ++ scanResults.getResult.asScala.toList
    if (newCursor != "0") scanOverHash(updatedKeys, newCursor, scanParams)
    else updatedKeys
  }
}
