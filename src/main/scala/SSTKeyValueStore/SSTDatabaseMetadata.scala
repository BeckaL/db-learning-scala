package SSTKeyValueStore

import model.LogFile

import scala.collection.immutable.TreeMap

case class SSTDatabaseMetadata(memTable: TreeMap[String, String], logFiles: List[LogFile])
