package Abgabe1

import scala.concurrent.Future

trait SimpleQueries {
  def close(): Future[Unit]
  def isConsistent(): Future[Boolean]
  def countGoals(name: String): Future[Int]
  def countRangeGoals(min: Int, max: Int): Future[Int]
}