package model

import java.nio.file.Path

case class DatabaseMetadata(path: Path, logFiles: List[LogFile], fileLimit: Long) {
  def withUpdatedLogFileIndex(indexMap: Map[String, Long]): DatabaseMetadata = {
    this.copy(logFiles = this.logFiles.updated(0, this.logFiles.head.copy(index = indexMap)))
  }
}
