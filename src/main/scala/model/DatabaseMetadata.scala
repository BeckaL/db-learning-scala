package model

import java.nio.file.Path

case class DatabaseMetadata(path: Path, indices: List[LogFile]) {
  def withUpdatedLogFileIndex(indexMap: Map[String, Long]): DatabaseMetadata = {
    this.copy(indices = this.indices.updated(0, this.indices.head.copy(index = indexMap)))
  }
}
