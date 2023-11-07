package Abgabe1

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.StdIn
import scala.util.{Failure, Success}

object Main {
  private val url = "jdbc:h2:/localhost/~/DB-IS;AUTO_SERVER=TRUE"
  private val h2Driver = "org.h2.Driver"

  def main(args: Array[String]): Unit = {
    val queryList = List(JDBCQueries(h2Driver, url), SlickQueries(h2Driver, url))
    for (queries <- queryList)
      execute(queries, "Gerd Müller", 15, 20)
    StdIn.readLine()
    //makes sure that the queries are closed after all other queries are executed.
    for (queries <- queryList)
      queries.close()
    StdIn.readLine()
  }

  private def execute(queries: SimpleQueries, name: String, min: Int, max: Int): Unit = {
    queries.countGoals(name).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"$name scored $value goals")
    }
    queries.isConsistent().onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(true) => println("no inconsistent records in table 'goalscorers' found...")
      case Success(false) => println("inconsistent records in table 'goalscorers' found.")
    }
    queries.countRangeGoals(min, max).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"number of games with at least $min and at most $max goals: $value")
    }
  }
}
