package SSTKeyValueStore

import cats.effect.unsafe.implicits.global
import model.{KeyNotFoundInIndices, LogFile, UnparseableBinaryString}
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
  "write" should "write to a memtable" in {
    val startMetadata = SSTDatabaseMetadata(databasePath, TreeMap(), List())
    write(startMetadata, myKey, myValue).unsafeRunSync().getRight shouldBe
      startMetadata.copy(memTable = TreeMap(myKey -> myValue))
  }

  it should "update an existing memtable value" in {
    val startMetadata = SSTDatabaseMetadata(databasePath, TreeMap(myKey -> "otherValue"), List())
    write(startMetadata, myKey, myValue).unsafeRunSync().getRight shouldBe
      startMetadata.copy(memTable = TreeMap(myKey -> myValue))
  }

  it should "compress if the existing memtable is over the size limit, write to file and then store in the new memtable" in {
    val memtable      = TreeMap.from((1 to 101).map(_.toString).map(key => key -> myValue).toMap)
    val startMetadata = SSTDatabaseMetadata(databasePath, memtable, List())

    val result = write(startMetadata, myKey, myValue, dbMetadata => secondLogFile).unsafeRunSync().getRight
    result.memTable shouldBe TreeMap(myKey -> myValue)
    result.logFiles.map(_.path) shouldBe List(secondLogFile)

    Files.readString(secondLogFile) should startWith(getStringToWrite("1", myValue).getRight)
  }

  it should "not compress if the existing memtable is over the size limit but the key is already in the memtable" in {
    val memtable      = TreeMap.from((1 to 101).map(_.toString).map(key => key -> "anotherValue").toMap)
    val startMetadata = SSTDatabaseMetadata(databasePath, memtable, List())

    val result = write(startMetadata, "1", myValue, dbMetadata => secondLogFile).unsafeRunSync().getRight
    Files.exists(secondLogFile) shouldBe false
    result.memTable shouldBe memtable.updated("1", myValue)
    result.logFiles shouldBe List()
  }

  "read" should "read from the map" in {
    val memTable = TreeMap(myKey -> myValue)
    read(SSTDatabaseMetadata(databasePath, memTable, List()), myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is directly in the index" in {
    Files.writeString(existingLogFile, getStringToWrite(myKey, myValue).getOrElse(throw new RuntimeException("oops")))
    val logFile  = LogFile(existingLogFile, Map(myKey -> 0))
    val metadata = SSTDatabaseMetadata(databasePath, TreeMap("anotherKey" -> "anotherValue"), List(logFile))

    read(metadata, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is directly in an older index" in {
    Files.writeString(existingLogFile, getStringToWrite(myKey, myValue).getOrElse(throw new RuntimeException("oops")))
    val logFile  = LogFile(existingLogFile, Map(myKey -> 0))
    val metadata = SSTDatabaseMetadata(databasePath, TreeMap("anotherKey" -> "anotherValue"), List(LogFile.empty(secondLogFile), logFile))

    read(metadata, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is not directly in the index" in {
    Files.writeString(existingLogFile, ReadingFixture.allKeysAndValuesOrderedString.mkString(""))

    val index: Map[String, Long] = Map("a" * 5 -> 0, "h" * 5 -> ReadingFixture.getOffsetOf("h"), "t" -> ReadingFixture.getOffsetOf("t"))

    val inMemoryIndex       = TreeMap("someKey" -> "someValue", "anotherKey" -> "anotherValue")
    val sstDatabaseMetadata = SSTDatabaseMetadata(databasePath, inMemoryIndex, List(LogFile(existingLogFile, index)))

    val keyToSearchFor = "r" * ReadingFixture.keySize
    val expectedValue  = "r" * ReadingFixture.valueSize

    read(sstDatabaseMetadata, keyToSearchFor).unsafeRunSync() shouldBe Right(expectedValue)
  }

  it should "read from a logfile when the entry is not directly in the memtable and is after the last key in the logfile index" in {
    Files.writeString(existingLogFile, ReadingFixture.allKeysAndValuesOrderedString.mkString(""))

    val index: Map[String, Long] = Map("a" * 5 -> 0, "h" * 5 -> ReadingFixture.getOffsetOf("h"), "t" -> ReadingFixture.getOffsetOf("t"))

    val inMemoryIndex       = TreeMap("someKey" -> "someValue", "anotherKey" -> "anotherValue")
    val sstDatabaseMetadata = SSTDatabaseMetadata(databasePath, inMemoryIndex, List(LogFile(existingLogFile, index)))

    val keyToSearchFor = "v" * ReadingFixture.keySize
    val expectedValue  = "v" * ReadingFixture.valueSize

    read(sstDatabaseMetadata, keyToSearchFor).unsafeRunSync() shouldBe Right(expectedValue)
  }

  it should "bubble up a lower level error" in {
    val firstKeyValue     = getStringToWrite("aaa", "aaaaa").getRight
    val malformedKeyValue = "00000010" + "a" + "00000001" + "a"
    val myKeyValue        = getStringToWrite(myKey, myValue).getRight

    val index: Map[String, Long] = Map("a" -> 0)

    Files.writeString(existingLogFile, firstKeyValue + malformedKeyValue + myKeyValue)
    val inMemoryIndex       = TreeMap("someKey" -> "someValue", "anotherKey" -> "anotherValue")
    val sstDatabaseMetadata = SSTDatabaseMetadata(databasePath, inMemoryIndex, List(LogFile(existingLogFile, index)))

    read(sstDatabaseMetadata, myKey).unsafeRunSync().getLeft shouldBe a[UnparseableBinaryString]
  }

  it should "return a left when the value is not found in the memtable and there are no logs" in {
    read(SSTDatabaseMetadata(databasePath, TreeMap(), List()), myKey).unsafeRunSync() shouldBe Left(KeyNotFoundInIndices(myKey))
  }

  "compressAndWriteMemTable" should "write the memtable to an ordered file" in {
    val m        = TreeMap.from(ReadingFixture.allKeysAndValuesOrdered)
    val metadata = SSTDatabaseMetadata(databasePath, m, List())
    val result   = compressAndWriteToSSTFile(metadata, metadata => secondLogFile).unsafeRunSync()

    // Currently, we just compress every ten values, rather than care about the size of the compressed block
    val expectedLogIndex: Map[String, Long] =
      Map("aaaaa" -> 0, "kkkkk" -> ReadingFixture.getOffsetOf("k"), "uuuuu" -> ReadingFixture.getOffsetOf("u"))

    Files.readString(secondLogFile) shouldBe ReadingFixture.allKeysAndValuesOrderedString.mkString("")
    result shouldBe Right(SSTDatabaseMetadata(databasePath, TreeMap(), List(LogFile(secondLogFile, expectedLogIndex))))
  }

  override def afterEach(): Unit = {
    Files.writeString(existingLogFile, "")
    Files.deleteIfExists(secondLogFile)
  }

  object ReadingFixture {
    val keySize   = 5
    val valueSize = 10
    val allKeysAndValuesOrdered = ('a' to 'z').toList
      .map(char => (char.toString * keySize, char.toString * valueSize))
    val allKeysAndValuesOrderedString =
      allKeysAndValuesOrdered.map((key, value) => getStringToWrite(key, value).getRight)

    def getOffsetOf(string: String) = {
      val keyValueStringLength = keySize + valueSize + 16
      ('a' to 'z').toList.indexOf(string.head) * keyValueStringLength
    }
  }

  implicit class EitherOps[A, B](e: Either[A, B]) {
    def getRight = e.getOrElse(throw new RuntimeException("expected right but got left"))

    def getLeft = e.left.getOrElse(throw new RuntimeException("expected left but got right"))
  }
}
