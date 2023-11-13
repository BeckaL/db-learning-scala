package SSTKeyValueStore

import model.LogFile

import java.nio.file.Path
import scala.collection.immutable.TreeMap

case class SSTDatabaseMetadata(path: Path, memTable: TreeMap[String, String], logFiles: List[LogFile]) {
  def withUpdatedKeyValue(key: String, value: String) = this.copy(memTable = memTable.updated(key, value))
}
