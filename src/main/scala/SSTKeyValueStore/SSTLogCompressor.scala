package SSTKeyValueStore

import cats.effect.IO
import model.LogFile

import java.nio.channels.FileChannel
import java.nio.file.Path

def compress(newerFile: Path, olderFile: Path, newFilePath: Path): IO[Unit] = {
  ???
}
