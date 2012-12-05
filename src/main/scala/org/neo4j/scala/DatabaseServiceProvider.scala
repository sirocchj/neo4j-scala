package org.neo4j.scala

import collection.JavaConversions.mutableMapAsJavaMap
import collection.mutable.{Map => MMap}
import java.io.File
import java.net.{MalformedURLException, URL}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.rest.graphdb.RestGraphDatabase
import org.neo4j.unsafe.batchinsert.BatchInserters
import scala.util.control.Exception._
import sys.ShutdownHookThread

/**
 * Interface for a GraphDatabaseServiceProvider
 * must be implemented by and Graph Database Service Provider
 */
trait GraphDatabaseServiceProvider {

  val removeOnShutdown = false

  def store: String

  def ds: DatabaseService

  implicit def wrapGraphDatabaseService(gds: GraphDatabaseService) = DatabaseServiceImpl(gds)

  implicit def unwrapGraphDatabaseService(ds: DatabaseService) = ds.gds

  implicit def dirToPimpedDir(storeDir: String) = new {

    def rmdir() {
      def rmdirRec(f: File) {
        f.listFiles().foreach {
          file =>
            if (file.isDirectory) rmdirRec(file)
            file.delete()
        }
        f.delete()
      }
      rmdirRec(new File(storeDir))
    }

  }

  private[scala] def registerShutdownHook() {
    ShutdownHookThread {
      ds.shutdown()
      if (removeOnShutdown) store.rmdir()
    }
  }

}

/**
 * provides a specific Database Service
 * in this case an embedded database service
 */
trait EmbeddedGraphDatabaseServiceProvider extends GraphDatabaseServiceProvider {

  /** Setup configuration parameters
    *
    * @return Map[String, String] configuration parameters
    */
  def configParams = Map[String, String]()

  /** Using an instance of an embedded graph database */
  lazy val ds: DatabaseService = {
    val ds = new EmbeddedGraphDatabase(store, MMap(configParams.toSeq: _*))
    registerShutdownHook()
    ds
  }

}

/** Provides a specific GraphDatabaseServiceProvider for
  * Batch processing
  */
trait BatchGraphDatabaseServiceProvider extends EmbeddedGraphDatabaseServiceProvider {

  override lazy val ds: DatabaseService = {
    val ds = BatchInserters.batchDatabase(store, MMap(configParams.toSeq: _*))
    registerShutdownHook()
    ds
  }

  /** Return instance of BatchInserter */
  lazy val inserter = {
    val inserter = BatchInserters.inserter(store, MMap(configParams.toSeq: _*))
    ShutdownHookThread {
      inserter.shutdown()
      if (removeOnShutdown) store.rmdir()
    }
    inserter
  }

}

/** The Java binding for the Neo4j Server REST API wraps the REST calls
  * behind the well known GraphDatabaseService API
  */
trait RestGraphDatabaseServiceProvider extends GraphDatabaseServiceProvider {

  def testStore(str: String): URL = {
    println(str)
    catching(classOf[MalformedURLException]).either {
      new URL(str)
    }.fold(
      fa => {
        sys.error(fa.getLocalizedMessage)
        sys.exit(-1)
      },
      fb => fb
    )
  }

  /** Has to be overwritten to define username and password
    *
    * @return Option[(String, String)] user and password as Option of Strings
    */
  def userPw: Option[(String, String)] = None

  /** Creates a new instance of a REST Graph Database Service */
  lazy val ds: DatabaseService = {
    val ds = userPw match {
      case None => new RestGraphDatabase(store.toString)
      case Some((u, p)) => new RestGraphDatabase(testStore(store).toString, u, p)
    }
    registerShutdownHook()
    ds
  }

}