package model

sealed trait Action
case object Quit                             extends Action
case object Unknown                          extends Action
case object Help                             extends Action
case class Store(key: String, value: String) extends Action
case class Read(key: String)                 extends Action
