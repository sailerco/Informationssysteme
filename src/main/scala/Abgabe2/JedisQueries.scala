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
    val keys = jedis.keys("results:*").toArray.map(_.asInstanceOf[String]).toSet
    //TODO: FIX THIS
    val consistent = keys.forall { key =>
      val home_goals_saved = jedis.hget(key, "home_goals")
      val away_goals_saved = jedis.hget(key, "away_goals")
      if (home_goals_saved != null && away_goals_saved != null)
        home_goals_saved.split(";").length.toString == jedis.hget(key, "home_score") && away_goals_saved.split(";").length.toString == jedis.hget(key, "away_score")
      else
        false
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
}
