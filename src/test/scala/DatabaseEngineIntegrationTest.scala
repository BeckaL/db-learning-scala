import cats.data.EitherT
import cats.effect.unsafe.implicits.global
import model.{DatabaseMetadata, LogFile, ReadTooSmallValue}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.nio.file.{Files, Paths}

class DatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TableDrivenPropertyChecks {
  private val logFileName               = "logFile1.txt"
  private val existingDatabasePath      = Paths.get("./src/test/resources/DatabaseEngineIntegrationTestDatabase")
  private val existingLogFilePath       = Paths.get(s"./src/test/resources/DatabaseEngineIntegrationTestDatabase/$logFileName")
  private val secondExistingLogFilePath = Paths.get(s"./src/test/resources/DatabaseEngineIntegrationTestDatabase/logFile2.txt")
  private val thirdLogFilePath          = Paths.get(s"./src/test/resources/DatabaseEngineIntegrationTestDatabase/logFile3.txt")
  private val myKey                     = "myKey"
  private val keySize                   = "00000101"
  private val myValue                   = "myValue"
  private val valueSize                 = "00000111"
  private val keyValueString            = keySize + myKey + valueSize + myValue
  private val newLogFilePath            = Paths.get(s"./src/test/resources/DatabaseEngineIntegrationTestDatabase/afterCompressLogFile.txt")

  "write" should "update the index when it is empty" in {
    val databaseEngine = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map())), 1000L)

    storeKeyValue(myKey, myValue, databaseEngine).unsafeRunSync() shouldBe
      Right(databaseEngine.withUpdatedLogFileIndex(Map(myKey -> 0)))
    Files.readString(existingLogFilePath) shouldBe keyValueString
  }

  it should "update the index when it is not empty" in {
    val existingData = "someKeyAndValue"
    Files.writeString(existingLogFilePath, existingData)
    val indexMap         = Map("otherKey" -> 0.toLong)
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, indexMap)), 1000L)

    storeKeyValue(myKey, myValue, databaseMetadata).unsafeRunSync() shouldBe
      Right(databaseMetadata.withUpdatedLogFileIndex(indexMap.updated(myKey, existingData.length)))

    Files.readString(existingLogFilePath) shouldBe existingData + keyValueString
  }

  it should "overwrite an existing entry when writing the same key to the same log file" in {
    Files.writeString(existingLogFilePath, keyValueString)
    val indexMap         = Map(myKey -> 0.toLong)
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, indexMap)), 1000L)

    storeKeyValue(myKey, myValue, databaseMetadata).unsafeRunSync() shouldBe
      Right(databaseMetadata.withUpdatedLogFileIndex(Map(myKey -> keyValueString.length)))
  }

  it should "write to a new file when the existing log file is at capacity" in {
    val fileLimit    = 1000
    val existingData = "a" * 999
    Files.writeString(secondExistingLogFilePath, existingData)
    val existingLogFiles   = List(LogFile(secondExistingLogFilePath, Map()), LogFile(existingLogFilePath, Map()))
    val databaseMetadata   = DatabaseMetadata(existingDatabasePath, existingLogFiles, fileLimit.toLong)
    val expectedNewLogFile = LogFile(thirdLogFilePath, Map(myKey -> 0))

    storeKeyValue(myKey, myValue, databaseMetadata).unsafeRunSync()
      .getRight
      .indices shouldBe expectedNewLogFile +: existingLogFiles
    Files.exists(thirdLogFilePath) shouldBe true
  }

  "getFromKey" should "store a key value and retrieve it" in {
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map())), 1000L)
    val secondKey        = "anotherKey"
    val secondValue      = "anotherValue"
    val result = (for {
      updatedMetadata1     <- EitherT.apply(storeKeyValue(myKey, myValue, databaseMetadata))
      updatedMetadata2     <- EitherT.apply(storeKeyValue(secondKey, secondValue, updatedMetadata1))
      firstRetrievedValue  <- EitherT.apply(getFromKey(myKey, updatedMetadata2))
      secondRetrievedValue <- EitherT.apply(getFromKey(secondKey, updatedMetadata2))
    } yield (firstRetrievedValue, secondRetrievedValue)).value.unsafeRunSync()
    val (firstRetrievedValue, secondRetrievedValue) = result.getOrElse(throw new RuntimeException("Didn't get a right"))

    result shouldBe Right((myValue, secondValue))
  }

  it should "retrieve a key when it is not found in the latest index" in {
    Files.writeString(secondExistingLogFilePath, keyValueString)
    val databaseMetadata = DatabaseMetadata(
      existingDatabasePath,
      List(
        LogFile(existingLogFilePath, Map("anotherKey" -> 0)),
        LogFile(secondExistingLogFilePath, Map(myKey -> 0))
      ),
      1000L
    )

    getFromKey(myKey, databaseMetadata).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "retrieve the latest key when is found in multiple indices" in {
    Files.writeString(existingLogFilePath, keyValueString)
    val otherValue = myValue.reverse
    Files.writeString(secondExistingLogFilePath, keySize + myKey + valueSize + otherValue)
    val databaseMetadata = DatabaseMetadata(
      existingDatabasePath,
      List(
        LogFile(secondExistingLogFilePath, Map(myKey -> 0)),
        LogFile(existingLogFilePath, Map(myKey -> 0))
      ),
      1000L
    )

    getFromKey(myKey, databaseMetadata).unsafeRunSync() shouldBe Right(otherValue)
  }

  it should "return a left with error message if the key is not in any index" in {
    Files.writeString(secondExistingLogFilePath, keyValueString)
    val databaseMetadata = DatabaseMetadata(
      existingDatabasePath,
      List(
        LogFile(existingLogFilePath, Map("anotherKey" -> 0)),
        LogFile(secondExistingLogFilePath, Map("andAnotherKey" -> 0))
      ),
      1000L
    )

    getFromKey(myKey, databaseMetadata)
      .unsafeRunSync()
      .getLeft
      .message shouldBe
      s"Could not find key $myKey in indices"
  }

  it should "return a left with error message when the index is incorrect" in {
    Files.writeString(existingLogFilePath, keyValueString)
    val differentKey     = "anotherKey"
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map(differentKey -> 0))), 1000L)

    getFromKey(differentKey, databaseMetadata).unsafeRunSync().getLeft
      .message shouldBe
      s"Expected to find key $differentKey in logfile $existingLogFilePath at index 0 but found $myKey, the index may be corrupted"
  }

  it should "return a left with error message when the stored data is corrupted" in {
    val notTheCorrectValue = "a"
    Files.writeString(existingLogFilePath, keySize + myKey + valueSize + notTheCorrectValue)
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map(myKey -> 0))), 1000L)

    getFromKey(myKey, databaseMetadata).unsafeRunSync().getLeft
      .message shouldBe
      s"Expected a string of size ${myValue.size} but got string of size ${notTheCorrectValue.length}"
  }

  "compress" should "successfully compress two files into one" in {
    val firstKeyString        = getStringToWrite("firstKey", "firstValue").getRight
    val secondKeyString       = getStringToWrite("secondKey", "secondValue").getRight
    val thirdKeyString        = getStringToWrite("thirdKey", "thirdValue").getRight
    val fourthKeyString       = getStringToWrite("fourthKey", "fourthValue").getRight
    val firstKeyStringUpdated = getStringToWrite("firstKey", "aDifferentFirstValue").getRight

    writeToFile(firstKeyString + secondKeyString + thirdKeyString, existingLogFilePath).unsafeRunSync()
    writeToFile(firstKeyStringUpdated + fourthKeyString, secondExistingLogFilePath).unsafeRunSync()

    val olderLogFile = LogFile(
      existingLogFilePath,
      Map(
        "firstKey"  -> 0,
        "secondKey" -> firstKeyString.length,
        "thirdKey"  -> (firstKeyString.length + secondKeyString.length)
      )
    )
    val newerLogFile     = LogFile(secondExistingLogFilePath, Map("firstKey" -> 0, "fourthKey" -> firstKeyStringUpdated.length))
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile.empty(thirdLogFilePath), newerLogFile, olderLogFile), 1000L)

    val updatedMetadata = compress(databaseMetadata, md => newLogFilePath).unsafeRunSync().getRight

    updatedMetadata.indices.size shouldBe 1
    updatedMetadata.indices.head.index.keys.toSet shouldBe Set("firstKey", "secondKey", "thirdKey", "fourthKey")
    updatedMetadata.indices.head.path shouldBe newLogFilePath

    List(existingLogFilePath, secondExistingLogFilePath).foreach(Files.exists(_) shouldBe false)

    val expectedValues = Table(
      ("key", "value"),
      ("firstKey", "aDifferentFirstValue"),
      ("secondKey", "secondValue"),
      ("thirdKey", "thirdValue"),
      ("fourthKey", "fourthValue")
    )

    expectedValues.forEvery((k, v) => getFromKey(k, updatedMetadata).unsafeRunSync().getRight shouldBe v)
  }

  it should "return an error if there are less than three log files" in {
    val databaseMetadata =
      DatabaseMetadata(existingDatabasePath, List(LogFile.empty(existingLogFilePath), LogFile.empty(secondExistingLogFilePath)), 1000L)

    compress(databaseMetadata).unsafeRunSync().getLeft.message shouldBe
      s"There were not enough log files to compress: need at least two dormant log files " +
      s"and one live file to compress, but there were only 2 files in total"
  }

  override def beforeEach(): Unit = {
    Files.createFile(existingLogFilePath)
    Files.createFile(secondExistingLogFilePath)
  }

  override def afterEach() = {
    Files.deleteIfExists(existingLogFilePath)
    Files.deleteIfExists(secondExistingLogFilePath)
    Files.deleteIfExists(thirdLogFilePath)
    Files.deleteIfExists(newLogFilePath)
  }

  implicit class EitherOps[A, B](e: Either[A, B]) {
    def getRight = e.getOrElse(throw new RuntimeException("expected right but got left"))

    def getLeft = e.left.getOrElse(throw new RuntimeException("expected left but got right"))
  }
}
