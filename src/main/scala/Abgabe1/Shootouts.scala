package Abgabe1

import slick.jdbc.H2Profile.api.{Table, Tag, _}

import java.sql.Date

class Shootouts(tag: Tag) extends Table[(Date, String, String, String)](tag, "Shootouts") {
  def date = column[Date]("Date")

  def homeTeam = column[String]("home_team")

  def awayTeam = column[String]("away_team")

  def winner = column[String]("winner")

  def * = (date, homeTeam, awayTeam, winner)
}

