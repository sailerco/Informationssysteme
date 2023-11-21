package Abgabe2

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.JedisPooled

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ListHasAsScala

object JedisQueries {
  def apply(host: String, port: Int): JedisQueries = new JedisQueries(host, port)
}

class JedisQueries(host: String, port: Int) extends SimpleQueries {
  val jedis = new JedisPooled(host, port)
  val goals: List[String] = scan(List(), ScanParams.SCAN_POINTER_START, "goal:*")

  override def close(): Future[Unit] = Future {
    jedis.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val pipeline = jedis.pipelined()

    //put all results_id that appear in the goals into an set -> removes duplicates
    val resultsKey = goals.map(r => pipeline.hget(r, "results_id")).toSet
    pipeline.sync()

    //retrieve results by the id in the resultsKey (meaning only results that also have goals)
    val results = resultsKey.map(_.get())

    //val scores = results.map(id => (id, pipeline.hget(s"results:$id", "home_score"), pipeline.hget(s"results:$id", "away_score")))
    val matches = results.map { r => (r, pipeline.hgetAll(s"results:$r")) }
    pipeline.sync()

    //reduce map to the id, home_score and away_score
    val scores = matches.map { case (id, hash) => (id, hash.get().get("home_score"), hash.get().get("away_score")) }
    //retrieve all the goals that happened at the given match
    val grouped = scores.map { case (id, home, away) => (home, pipeline.zrangeByScore("home_goals", id, id), away, pipeline.zrangeByScore("away_goals", id, id)) }
    pipeline.sync()

    //check if an inconsistency exists
    !grouped.exists {
      case (home_score, home_goal, away_score, away_goal) =>
        home_score.toInt != home_goal.get().toArray.length && away_score.toInt != away_goal.get().toArray.length
    }
  }

  override def countGoals(name: String): Future[Int] = Future {
    //jedis.zscore("Scorer", name).toInt
    val pipeline = jedis.pipelined()
    val scorer = goals.map { key => pipeline.hget(key, "scorer") }
    pipeline.sync()
    scorer.count(_.get() == name)
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    /*val keys = jedis.zrangeWithScores("TotalGoals", 0, -1).asScala.toList
    keys
      .filter(k => (min to max).contains(k.getElement.toInt))
      .map(_.getScore).sum.toInt*/
    val pipeline = jedis.pipelined()
    val cursor = ScanParams.SCAN_POINTER_START
    val results = scan(List(), cursor, "results:*")
    val scores = results.map { key => pipeline.hgetAll(key) }
    pipeline.sync()
    scores.count(entries =>
      (min to max).contains(entries.get().get("home_score").toInt + entries.get().get("away_score").toInt)
    )
  }

  @tailrec
  private def scan(all: List[String], cursor: String, pattern: String): List[String] = {
    val result = jedis.scan(cursor, new ScanParams().`match`(pattern).count(1000))
    val new_cursor = result.getCursor
    val new_list = all ++ result.getResult.asScala.toList
    if (new_cursor != "0") {
      scan(new_list, new_cursor, pattern)
    } else {
      new_list
    }
  }
}
