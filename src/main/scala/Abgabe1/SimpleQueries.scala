package Abgabe1

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import slick.jdbc.H2Profile.api

import java.sql.DriverManager

object Query {
  def apply(): Query = {
    new Query()
  }
}
class Query extends SimpleQueries {
  private val connection = DriverManager.getConnection("jdbc:h2:/localhost/~/DB-IS", "sa", "123")

  override def close(): Future[Unit] = ??? /*{
  if(!connection.isClosed)
      connection.close()
  }*/

  override def isConsistent(): Future[Boolean] = ??? /* {
    val goalscorers = Data().getGoalscorer(connection)
    val results = Data().getResult(connection)
  }*/

  override def countGoals(name: String): Future[Int] = {
    Data().countGoalscorerName(name, connection)
  }
  override def countRangeGoals(min: Int, max: Int): Future[Int] = ???
}

trait SimpleQueries {
  def close(): Future[Unit]
  def isConsistent(): Future[Boolean]
  def countGoals(name: String): Future[Int]
  def countRangeGoals(min: Int, max: Int): Future[Int]
}

class JDBCQueries(driver: String, url: String) extends SimpleQueries{
  val connection = DriverManager.getConnection(url)
  override def close(): Future[Unit] = {
    connection.close()
    Future.successful(())
  }

  override def isConsistent(): Future[Boolean] = ???

  override def countGoals(name: String): Future[Int] = ???

  override def countRangeGoals(min: Int, max: Int): Future[Int] = ???
}

class SlickQueries(driver: String, url: String) extends SimpleQueries {
  override def close(): Future[Unit] = ???

  override def isConsistent(): Future[Boolean] = ???

  override def countGoals(name: String): Future[Int] = ???

  override def countRangeGoals(min: Int, max: Int): Future[Int] = ???
}