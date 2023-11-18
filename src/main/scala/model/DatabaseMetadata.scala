package model

import java.nio.file.Path
import scala.collection.immutable.TreeMap

trait DatabaseMetadata { def logFiles: List[LogFile] }

case class SimpleDatabaseMetadata(path: Path, logFiles: List[LogFile], fileLimit: Long) extends DatabaseMetadata {
  def withUpdatedLogFileIndex(indexMap: Map[String, Long]): SimpleDatabaseMetadata = {
    this.copy(logFiles = this.logFiles.updated(0, this.logFiles.head.copy(index = indexMap)))
  }
}

case class SSTDatabaseMetadata(path: Path, memTable: TreeMap[String, String], logFiles: List[LogFile]) extends DatabaseMetadata {
  def withUpdatedKeyValue(key: String, value: String) = this.copy(memTable = memTable.updated(key, value))
}
