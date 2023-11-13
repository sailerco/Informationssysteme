package Abgabe2

import redis.clients.jedis.{Jedis, Pipeline}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object JedisQueries {
  def apply(jedis: Jedis, pipeline: Pipeline): JedisQueries = new JedisQueries(jedis, pipeline)
}

class JedisQueries(jedis: Jedis, pipeline: Pipeline) extends SimpleQueries {
  override def close(): Future[Unit] = Future {
    pipeline.close()
    jedis.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val pipeline = jedis.pipelined()
    val goalIds = jedis.keys("goal:*").toArray.map(_.asInstanceOf[String]).toSet
    val consistent = goalIds.forall { key =>
      val resultKey = s"results:" + jedis.hget(key, "results_id")
      val homeGoalsCount = getCount(jedis.hget(resultKey, "home_goals"))
      val awayGoalsCount = getCount(jedis.hget(resultKey, "away_goals"))
      homeGoalsCount == jedis.hget(resultKey, "home_score").toInt && awayGoalsCount == jedis.hget(resultKey, "away_score").toInt
    }
    pipeline.close()
    consistent
  }

  override def countGoals(name: String): Future[Int] = Future {
    val pipeline = jedis.pipelined()
    val keys = jedis.keys("goal:*").toArray.map(_.asInstanceOf[String]).toSet
    val count = keys.foldLeft(0) { (n, key) =>
      val scorer_name = pipeline.hget(key, "scorer_id")
      pipeline.sync()
      if (("scorer:" + name).equals(scorer_name.get()))
        n + 1
      else
        n
    }
    pipeline.close()
    count
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val pipeline = jedis.pipelined()
    val keys = jedis.keys("results:*").toArray.map(_.asInstanceOf[String]).toSet

    val count = keys.foldLeft(0) { (n, key) =>
      val home_score = pipeline.hget(key, "home_score")
      val away_score = pipeline.hget(key, "away_score")
      pipeline.sync()
      val combined_score = home_score.get().toInt + away_score.get().toInt
      if (min <= combined_score && combined_score <= max)
        n + 1
      else
        n
    }

    pipeline.close()
    count
  }

  private def getCount(text: String): Int = {
    if (text != null && text.contains(";")) {
      text.split(";").length
    } else if (text != null && !text.contains(";"))
      1
    else
      0
  }
}
