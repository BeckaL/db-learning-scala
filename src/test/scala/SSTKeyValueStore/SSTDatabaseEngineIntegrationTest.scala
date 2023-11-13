package SSTKeyValueStore

import cats.effect.unsafe.implicits.global
import model.{KeyNotFoundInIndices, LogFile}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import shared.getStringToWrite

import java.nio.file.{Files, Paths}
import scala.collection.immutable.TreeMap

class SSTDatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val myKey           = "myKey"
  private val myValue         = "myValue"
  private val databasePath    = Paths.get("./src/test/resources/SSTDatabaseEngineIntegrationTest")
  private val existingLogFile = Paths.get(databasePath.toString + "/" + "logFile1.txt")
  private val secondLogFile   = Paths.get(databasePath.toString + "/" + "logFile2.txt")
  "write" should "write to a thing" in {
    write(SSTDatabaseMetadata(TreeMap(), List()), myKey, myValue) shouldBe TreeMap(myKey -> myValue)
  }

  "read" should "read from the map" in {
    val memTable = TreeMap(myKey -> myValue)
    read(SSTDatabaseMetadata(memTable, List()), myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is directly in the index" in {
    Files.writeString(existingLogFile, getStringToWrite(myKey, myValue).getOrElse(throw new RuntimeException("oops")))
    val logFile  = LogFile(existingLogFile, Map(myKey -> 0))
    val metadata = SSTDatabaseMetadata(TreeMap("anotherKey" -> "anotherValue"), List(logFile))

    read(metadata, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is directly in an older index" in {
    Files.writeString(existingLogFile, getStringToWrite(myKey, myValue).getOrElse(throw new RuntimeException("oops")))
    val logFile  = LogFile(existingLogFile, Map(myKey -> 0))
    val metadata = SSTDatabaseMetadata(TreeMap("anotherKey" -> "anotherValue"), List(LogFile.empty(secondLogFile), logFile))

    read(metadata, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is not directly in the index" in {
    val keySize   = 5
    val valueSize = 10
    val toWrite = ('a' to 'z').toList
      .map(char => getStringToWrite(char.toString * keySize, char.toString * valueSize).getRight)
    Files.writeString(existingLogFile, toWrite.mkString(""))

    def getOffsetOf(string: String) = {
      val keyValueStringLength = keySize + valueSize + 16
      ('a' to 'z').toList.indexOf(string.head) * keyValueStringLength
    }

    val index: Map[String, Long] = Map("a" * 5 -> 0, "h" * 5 -> getOffsetOf("h"), "t" -> getOffsetOf("t"))
    println(index)
    println(getOffsetOf("b"))

    val logFile             = LogFile(existingLogFile, index)
    val inMemoryIndex       = TreeMap("someKey" -> "someValue", "anotherKey" -> "anotherValue")
    val sstDatabaseMetadata = SSTDatabaseMetadata(inMemoryIndex, List(logFile))

    val keyToSearchFor = "r" * 5
    val expectedValue  = "r" * 10

    read(sstDatabaseMetadata, keyToSearchFor).unsafeRunSync() shouldBe Right(expectedValue)
  }

  it should "return a left when the value is not found in the memtable and there are no logs" in {
    read(SSTDatabaseMetadata(TreeMap(), List()), myKey).unsafeRunSync() shouldBe Left(KeyNotFoundInIndices(myKey))
  }

  override def afterEach(): Unit = {
    Files.writeString(existingLogFile, "")
  }

  implicit class EitherOps[A, B](e: Either[A, B]) {
    def getRight = e.getOrElse(throw new RuntimeException("expected right but got left"))

    def getLeft = e.left.getOrElse(throw new RuntimeException("expected left but got right"))
  }
}
