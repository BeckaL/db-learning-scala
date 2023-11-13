package SSTKeyValueStore

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import shared.getStringToWrite
import cats.effect.unsafe.implicits.global
import utils.TestUtils

import java.nio.file.{Files, Paths}

class LogCompressorIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestUtils {
  private val myKey         = "myKey"
  private val myValue       = "myValue"
  private val databasePath  = Paths.get("./src/test/resources/LogCompressorIntegrationTest")
  private val firstLogFile  = Paths.get(databasePath.toString + "/" + "logFile1.txt")
  private val secondLogFile = Paths.get(databasePath.toString + "/" + "logFile2.txt")
  private val newLogFile    = Paths.get(databasePath.toString + "/" + "logFile3.txt")

  "compress" should "merge two files with different values" in {
    val newerLogFileValues = List("a", "c", "e", "g")
    val olderLogFileValues = List("b", "d", "f", "h")

    Files.writeString(firstLogFile, getNewerLogFileLines(newerLogFileValues))
    Files.writeString(secondLogFile, getOlderLogFileLines(olderLogFileValues))

    val result = compress(firstLogFile, secondLogFile, newLogFile).unsafeRunSync()

    val expectedValues = List(
      "a" -> newerLogFileValue("a"),
      "b" -> olderLogFileValue("b"),
      "c" -> newerLogFileValue("c"),
      "d" -> olderLogFileValue("d"),
      "e" -> newerLogFileValue("e"),
      "f" -> olderLogFileValue("f"),
      "g" -> newerLogFileValue("g"),
      "h" -> olderLogFileValue("h")
    ).map { case (key, value) => getStringToWrite(key, value).getRight }.mkString("")

    Files.readString(newLogFile) shouldBe expectedValues
  }

  it should "merge two files when some of the keys are overlapping, picking the newer in this case" in {
    val newerLogFileValues = List("a", "c", "e", "f", "h")
    val olderLogFileValues = List("a", "b", "d", "f", "g")

    val newerStringToWrite = newerLogFileValues.map(key =>
      getStringToWrite(key, newerLogFileValue(key)).getOrElse(throw new RuntimeException("Uh oh"))
    ).mkString("")

    val olderStringToWrite = olderLogFileValues.map(key =>
      getStringToWrite(key, olderLogFileValue(key)).getOrElse(throw new RuntimeException("Uh oh"))
    ).mkString("")

    Files.writeString(firstLogFile, newerStringToWrite)
    Files.writeString(secondLogFile, olderStringToWrite)

    compress(firstLogFile, secondLogFile, newLogFile).unsafeRunSync()
    val expectedValues = List(
      "a" -> newerLogFileValue("a"),
      "b" -> olderLogFileValue("b"),
      "c" -> newerLogFileValue("c"),
      "d" -> olderLogFileValue("d"),
      "e" -> newerLogFileValue("e"),
      "f" -> newerLogFileValue("f"),
      "g" -> olderLogFileValue("g"),
      "h" -> newerLogFileValue("h")
    )
      .map { case (key, value) => getStringToWrite(key, value).getOrElse(throw new RuntimeException("blah")) }.mkString("")
    Files.readString(newLogFile) shouldBe expectedValues
  }

  override def beforeEach(): Unit = {
    Files.createFile(firstLogFile)
    Files.createFile(secondLogFile)
  }

  override def afterEach(): Unit = {
    Files.deleteIfExists(firstLogFile)
    Files.deleteIfExists(secondLogFile)
    Files.deleteIfExists(newLogFile)
  }

  private def getNewerLogFileLines(keys: List[String]): String =
    keys.map(k => getStringToWrite(k, newerLogFileValue(k)).getRight).mkString("")

  private def getOlderLogFileLines(keys: List[String]): String =
    keys.map(k => getStringToWrite(k, olderLogFileValue(k)).getRight).mkString("")
  private def newerLogFileValue(key: String) = key * 2
  private def olderLogFileValue(key: String) = key * 5
}
