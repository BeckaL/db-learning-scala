package SSTKeyValueStore

import model.KeyNotFoundInIndices
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class SSTDatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers {
  private val myKey   = "myKey"
  private val myValue = "myValue"
  "write" should "write to a thing" in {
    write(TreeMap(), myKey, myValue) shouldBe TreeMap(myKey -> myValue)
  }

  "read" should "read from the map" in {
    val tMap = TreeMap(myKey -> myValue)
    read(tMap, myKey) shouldBe Right(myValue)
  }

  it should "return a left when the value is not found" in {
    read(TreeMap(), myKey) shouldBe Left(KeyNotFoundInIndices(myKey))
  }
}
