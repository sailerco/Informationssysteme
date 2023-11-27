package Abgabe2


import redis.clients.jedis.Pipeline

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn
import scala.util.{Failure, Success}


object Main {
  val host = "localhost"
  val port = 6379
  def main(args: Array[String]): Unit = {
    //GenerateData(host, port).generate()

    val queries = JedisQueries(host, port)
    execute(queries, "Gerd Müller", 15, 20)
    StdIn.readLine()
    queries.close()
  }

  private def execute(queries: SimpleQueries, name: String, min: Int, max: Int): Unit = {
    queries.countGoals(name).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"$name scored $value goals")
    }

    queries.countRangeGoals(min, max).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"number of games with at least $min and at most $max goals: $value")
    }

    queries.isConsistent().onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(true) => println("no inconsistent records in table 'goalscorers' found...")
      case Success(false) => println("inconsistent records in table 'goalscorers' found.")
    }
  }
}

