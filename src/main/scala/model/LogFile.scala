package model

import java.nio.file.Path

case class LogFile(path: Path, index: Map[String, Long])
