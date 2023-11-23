package Abgabe2

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.params.ScanParams

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
  private val results_keys: List[String] = scan(List(), cursor, "results:*")
  private val goals_keys: List[String] = scan(List(), cursor, "goal:*")

  override def close(): Future[Unit] = Future {
    jedis.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val pipeline = jedis.pipelined()

    //put all results_id that appear in the goals into an set -> removes duplicates
    val results_of_goals = goals_keys.map(r => pipeline.hget(r, "results_id")).toSet
    pipeline.sync()

    //retrieve scores of result (meaning only results that also have goals)
    val results = results_of_goals.map(_.get())
    val scores = results.map(id =>
      (id, pipeline.hget(s"results:$id", "home_score"), pipeline.hget(s"results:$id", "away_score")))
    pipeline.sync()

    //retrieve all goals that happened at the given match
    val grouped = scores.map { case (id, home, away) =>
      (home.get(), pipeline.zrangeByScore("home_goals", id, id),
        away.get(), pipeline.zrangeByScore("away_goals", id, id))
    }
    pipeline.sync()

    //TODO: && or ||
    !grouped.exists { case (home_score, home_goal, away_score, away_goal) =>
      (home_score.toInt != home_goal.get().toArray.length
        && away_score.toInt != away_goal.get().toArray.length)
    }
  }

  override def countGoals(name: String): Future[Int] = Future {
    val pipeline = jedis.pipelined()
    val scorer = goals_keys.map { key => pipeline.hget(key, "scorer") }
    pipeline.sync()
    scorer.count(_.get() == name)
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val pipeline = jedis.pipelined()
    val scores = results_keys.map(key => (pipeline.hget(key, "home_score"), pipeline.hget(key, "away_score")))
    pipeline.sync()
    scores.count { case (home_score, away_score) => (min to max).contains(home_score.get().toInt + away_score.get().toInt)
    }
  }

  @tailrec
  private def scan(curr: List[String], cursor: String, pattern: String): List[String] = {
    val scan_results = jedis.scan(cursor, new ScanParams().`match`(pattern).count(1000))
    val new_cursor = scan_results.getCursor
    val updated_keys = curr ++ scan_results.getResult.asScala.toList
    if (new_cursor != "0") {
      scan(updated_keys, new_cursor, pattern)
    } else {
      updated_keys
    }
  }
}
