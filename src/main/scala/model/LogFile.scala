package model

import java.nio.file.Path

case class LogFile(path: Path, index: Map[String, Long])

object LogFile {
  def empty(path: Path): LogFile = LogFile(path, Map())
}
