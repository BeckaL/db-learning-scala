package model

import java.nio.file.Path

trait DatabaseException { val message: String }

case class KeyNotFoundInIndices(expectedKey: String) extends DatabaseException {
  val message = s"Could not find key $expectedKey in indices"
}

case class FoundUnexpectedKeyAtOffset(key: String, path: Path, offset: Long, retrievedKey: String) extends DatabaseException {
  val message = s"Expected to find key $key in logfile $path at index $offset but found $retrievedKey, the index may be corrupted"
}

case class ReadTooSmallValue(expectedSize: Int, actualSize: Int) extends DatabaseException {
  val message = s"Expected a string of size $expectedSize but got string of size $actualSize"
}

case class UnparseableBinaryString(s: String) extends DatabaseException {
  val message = s"Couldn't parse $s as binary string"
}

case class BinaryStringLengthExceeded(i: Int) extends DatabaseException {
  val message = s"Tried to store string of length $i, this was more than the allowed size"
}
