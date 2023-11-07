import java.sql.DriverManager
object main {
  private val connection = DriverManager.getConnection("jdbc:h2:./data/demo")
  private val preparedInsert = connection.prepareStatement("insert into person values(?, ?)")

  private def insert(id: Int, name: String): Unit = {
    preparedInsert.setInt(1, id)
    preparedInsert.setString(2, name)
    preparedInsert.execute()
  }
  def main(args: Array[String]): Unit = {
    val statement = connection.createStatement()
    statement.execute("drop table person")
    statement.execute("create table person(id int, name varchar(20))")

    val pairs = List("Donald", "Daisy", "Mickey", "Minnie")
      .zipWithIndex
    for(pair<-pairs)
      insert(pair._2, pair._1)
    //insert(23, "Daisy")


    //statement.execute("insert into person values(1, 'donald')")
    val result = statement.executeQuery("select id, name from person")
    while(result.next()){
      val id = result.getInt(1) //zählung startet bei 1
      val name = result.getString(2)
      println(s"$id - $name")
    }
    statement.close()
    connection.close()
  }
}
