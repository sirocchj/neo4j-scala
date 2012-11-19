package org.neo4j.scala

import org.neo4j.graphdb.GraphDatabaseService

/** Interface for GraphDatabaseService
  *
  * @author Christopher Schmidt
  */
sealed trait DatabaseService {

  def gds: GraphDatabaseService

}

/** Wrapper around GraphDatabaseService */
case class DatabaseServiceImpl(gds: GraphDatabaseService) extends DatabaseService