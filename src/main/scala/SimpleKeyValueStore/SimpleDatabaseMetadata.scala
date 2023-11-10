package SimpleKeyValueStore

import model.LogFile

import java.nio.file.Path

case class SimpleDatabaseMetadata(path: Path, logFiles: List[LogFile], fileLimit: Long) {
  def withUpdatedLogFileIndex(indexMap: Map[String, Long]): SimpleDatabaseMetadata = {
    this.copy(logFiles = this.logFiles.updated(0, this.logFiles.head.copy(index = indexMap)))
  }
}
