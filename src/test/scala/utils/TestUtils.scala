package utils

trait TestUtils {
  val myKey   = "myKey"
  val myValue = "myValue"
  implicit class EitherOps[A, B](e: Either[A, B]) {
    def getRight: B = e.getOrElse(throw new RuntimeException(s"called get right on a left $e"))
    def getLeft: A  = e.left.getOrElse(throw new RuntimeException(s"called get left on a right $e"))
  }
}
