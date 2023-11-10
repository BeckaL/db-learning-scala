import SimpleKeyValueStore.SimpleDatabaseMetadata
import cats.effect.unsafe.implicits.global
import model.{KeyNotFoundInIndices, LogFile, UnparseableBinaryString}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import shared.*

import java.nio.file.{Files, Path, Paths}

class DatabaseFileUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TableDrivenPropertyChecks {
  private val prefix               = "./src/test/resources"
  private val logFile              = "logFile1.txt"
  private val databaseName         = "myDatabase"
  private val existingDatabasePath = Paths.get(s"$prefix/DatabaseFileUtilsTestDatabase")
  private val databasePath         = Paths.get(s"$prefix/$databaseName")
  private val logFilePath          = Paths.get(s"$prefix/$databaseName/$logFile")
  private val existingLogFilePath  = Paths.get(s"$prefix/DatabaseFileUtilsTestDatabase/$logFile")
  private val myKey                = "myKey"
  private val keySize              = "00000101"
  private val myValue              = "myValue"
  private val valueSize            = "00000111"
  private val newLogFilePath       = Paths.get(databasePath.toString + "/logFile2.txt")

  "create database engine" should "create a new folder and log file" in {
    val expectedDatabase = SimpleDatabaseMetadata(databasePath, List(LogFile(logFilePath, Map())), 1000L)

    createDatabaseEngine(prefix, "myDatabase", 1000L).unsafeRunSync() shouldBe expectedDatabase

    Files.isDirectory(databasePath) shouldBe true
    Files.isRegularFile(logFilePath) shouldBe true
  }

  "create new logfile" should "create a new log file prepended to the existing log files" in {
    val initialLogFile   = LogFile(logFilePath, Map())
    val expectedDatabase = SimpleDatabaseMetadata(databasePath, List(initialLogFile), 1000L)
    val metadata         = createDatabaseEngine(prefix, "myDatabase", 1000L).unsafeRunSync()

    val updatedMetadata = createNewLogFile(metadata).unsafeRunSync()

    val expectedNewLogFile = LogFile(newLogFilePath, Map())
    updatedMetadata.logFiles shouldBe List(expectedNewLogFile, initialLogFile)
    Files.isRegularFile(newLogFilePath) shouldBe true
  }

  "writeToFile" should "write to file and return the log file it wrote to" in {
    val stringToWrite = "someStringWithIndices"

    writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync() shouldBe 0
    Files.readString(existingLogFilePath) shouldBe stringToWrite
  }

  it should "write to a file with existing content in and return the log file it wrote to" in {
    val stringToWrite = "someStringWithIndices"

    val i1 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()
    val i2 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()

    Files.readString(existingLogFilePath) shouldBe stringToWrite + stringToWrite
    i1 shouldBe 0
    i2 shouldBe stringToWrite.size
  }

  "readFromFile" should "read bytes from a file" in {
    val stringToWrite = keySize + myKey + valueSize + myValue

    writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()

    readFromFile(0, existingLogFilePath).unsafeRunSync() shouldBe Right((myKey, myValue))
  }

  it should "read bytes from a file when the offset is greater than 0" in {
    val firstKeyValueString = "someKeyValueString"
    val otherKeyValueString = keySize + myKey + valueSize + myValue

    writeToFile(firstKeyValueString + otherKeyValueString, existingLogFilePath).unsafeRunSync()

    readFromFile(firstKeyValueString.length, existingLogFilePath).unsafeRunSync() shouldBe Right((myKey, myValue))
  }

  it should "return an error if the keySize or valueSize is not an 8 padded length binary string" in {
    val notACorrectlySizedBinaryString = "000"

    val incorrectlyFormattedThings = List(
      notACorrectlySizedBinaryString + myKey + valueSize + myValue,
      keySize + myKey + notACorrectlySizedBinaryString + myValue
    )

    incorrectlyFormattedThings.foreach(s => {
      writeToFile(s, existingLogFilePath).unsafeRunSync()
      readFromFile(0, existingLogFilePath).unsafeRunSync().getLeft
        .message should fullyMatch regex "Couldn't parse .+ as binary string".r
      Files.writeString(existingLogFilePath, "")
    })
  }

  it should "return an error if the file ends before a full value is read" in {
    val notACorrectlySizedBinaryString = "000"

    val notBigEnoughValue = "a"
    val notBigEnoughSize  = "000"
    val incorrectlyFormattedThings = Table(
      ("string", "expectedSize", "stringThatsNotBigEnoughSize"),
      (notBigEnoughSize, 8, 3),
      (keySize + notBigEnoughValue, 5, 1),
      (keySize + myKey + notBigEnoughSize, 8, 3),
      (keySize + myKey + valueSize + notBigEnoughValue, 7, 1)
    )

    incorrectlyFormattedThings.foreach((s, expectedSize, stringNotBigEnoughSize) => {
      writeToFile(s, existingLogFilePath).unsafeRunSync()
      readFromFile(0, existingLogFilePath).unsafeRunSync()
        .getLeft
        .message shouldBe s"Expected a string of size $expectedSize but got string of size $stringNotBigEnoughSize"
      Files.writeString(existingLogFilePath, "")
    })
  }

  "scanFileForKey" should "correctly retrieve a key" in {
    val firstString        = getStringToWrite("someKey", "someValue").getRight
    val secondString       = getStringToWrite("anotherKey", "anotherValue").getRight
    val searchingForString = getStringToWrite(myKey, myValue).getRight
    val fourthString       = getStringToWrite("fourthString", "fourthString").getRight
    val fifthString        = getStringToWrite("fifthString", "fifthValue").getRight

    val withinSegment = firstString + secondString + searchingForString + fourthString

    Files.writeString(existingLogFilePath, withinSegment + fifthString)

    scanFileForKey(0, withinSegment.size, existingLogFilePath, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "return left when a key is not found in the segment" in {
    val firstString   = getStringToWrite("someKey", "someValue").getRight
    val secondString  = getStringToWrite("anotherKey", "anotherValue").getRight
    val thirdString   = getStringToWrite("thirdString", "thirdValue").getRight
    val withinSegment = firstString + secondString + thirdString
    val fourthString  = getStringToWrite("fourthString", "fourthString").getRight
    Files.writeString(existingLogFilePath, withinSegment + fourthString)

    scanFileForKey(0, withinSegment.size, existingLogFilePath, myKey).unsafeRunSync() shouldBe Left(KeyNotFoundInIndices(myKey))
  }

  it should "bubble up a lower level error" in {
    val firstString  = getStringToWrite("someKey", "someValue").getRight
    val secondString = "000" + "keyForMalformattedEntry" + valueSize + myValue
    val thirdString  = getStringToWrite(myKey, myValue).getRight
    val fourthString = getStringToWrite("fourthString", "fourthString").getRight

    val withinSegment = firstString + secondString + thirdString + fourthString

    Files.writeString(existingLogFilePath, withinSegment)

    scanFileForKey(0, withinSegment.size, existingLogFilePath, myKey).unsafeRunSync().getLeft shouldBe a[UnparseableBinaryString]
  }

  override def afterEach(): Unit = {
    Files.deleteIfExists(logFilePath)
    Files.deleteIfExists(newLogFilePath)
    Files.deleteIfExists(databasePath)
    Files.writeString(existingLogFilePath, "")
  }

  implicit class EitherOps[A, B](e: Either[A, B]) {
    def getRight = e.getOrElse(throw new RuntimeException("expected right but got left"))
    def getLeft  = e.left.getOrElse(throw new RuntimeException("expected left but got right"))
  }
}
