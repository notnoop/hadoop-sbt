sbtPlugin := true

name := "hadoop-sbt"

organization := "com.notnoop"

version := "0.0.1-SNAPSHOT"

scalacOptions := Seq("-deprecation", "-unchecked")

seq(ScriptedPlugin.scriptedSettings: _*)

