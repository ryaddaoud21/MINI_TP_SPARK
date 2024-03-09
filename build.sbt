import sbt.Keys.libraryDependencies

import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "MiniProjet_SPARK",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
      "org.scalanlp" %% "breeze" % "1.2",
      "org.scalanlp" %% "breeze-viz" % "1.2",
      "org.apache.maven.plugins" % "maven-surefire-plugin" % "2.22.2",
      "org.scalameta" %% "munit" % "0.7.29" % Test ,
      "org.scalanlp" %% "breeze" % "2.1.0",
      "com.crealytics" %% "spark-excel" % "3.5.0_0.20.3",
      "com.crealytics" %% "spark-excel" % "0.13.1",

      // The visualization library is distributed separately as well.
      // It depends on LGPL code
      "org.scalanlp" %% "breeze-viz" % "2.1.0",
      "org.apache.poi" % "poi" % "5.2.2",
      "org.apache.poi" % "poi-ooxml" % "5.2.2",

    )
  )






