package model

import java.nio.file.Path

case class DatabaseMetadata(path: Path, indices: List[LogFile])
