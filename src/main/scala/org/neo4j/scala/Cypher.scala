package org.neo4j.scala

import org.neo4j.cypher.{ExecutionEngine, ExecutionPlan, ExecutionResult}
import java.io.PrintWriter
import org.neo4j.graphdb.PropertyContainer

/** Add additional as[T] interface where T has to be a case class */
trait TypedExecutionResult extends ExecutionResult {

  /** Maps a given column that has to be a property container to a case class
    *
    * @param column name of the column
    * @tparam T type of case class
    * @return Iterator[T]
    */
  def asCC[T: Manifest](column: String): Iterator[T]
}

/** Wraps the Cypher class ExecutionResult and provides an additional
  * Interface to access unmarshaled classes
  *
  * @param er ExecutionResult original execution result
  */
class TypedExecutionResultImpl(val er: ExecutionResult) extends TypedExecutionResult {

  def hasNext = er.hasNext

  def next() = er.next()

  def columns = er.columns

  def symbols = er.symbols

  def javaColumns = er.javaColumns

  def javaColumnAs[T](column: String) = er.javaColumnAs[T](column)

  def columnAs[T](column: String) = er.columnAs[T](column)

  def javaIterator = er.javaIterator

  def dumpToString(writer: PrintWriter) {
    er.dumpToString(writer)
  }

  lazy val dumpToString = er.dumpToString()

  lazy val queryStatistics = er.queryStatistics()

  /** Maps a given column that has to be a property container to a case class
    *
    * @param column name of the column
    * @tparam T type of case class
    * @return Iterator[T]
    */
  def asCC[T: Manifest](column: String): Iterator[T] = {
    new TypedPropertyContainerIterator(er.columnAs[PropertyContainer](column)).iterator
  }

}

/** Main Cypher support trait */
trait Cypher {

  self: Neo4jWrapper =>

  lazy val engine = new ExecutionEngine(ds.gds)

  implicit def toTypedExecutionResult(executionResult: ExecutionResult) = new TypedExecutionResultImpl(executionResult)

  implicit def toCypherQuery(query: => String) = new {

    def prepare = engine.prepare(query)

    def execute = engine.execute(query)

  }

}