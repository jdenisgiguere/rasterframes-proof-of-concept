name := "rasterframes-proof-of-concept"
version := "0.1.0"
scalaVersion := "2.11.8"
organization := "com.olameter"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature")
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

shellPrompt := { s => Project.extract(s).currentProject.id + " > " }

// We need to bump up the memory for some of the examples working with the landsat image.
javaOptions += "-Xmx4G"

fork in run := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true

libraryDependencies ++= Seq(
  "org.locationtech.sfcurve" %% "sfcurve-zorder" % "0.2.1-SNAPSHOT",
  "org.locationtech.rasterframes" %% "rasterframes" % "0.7.0",
  "org.locationtech.rasterframes" %% "rasterframes-datasource" % "0.7.0",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "com.typesafe.akka" %% "akka-actor"  % "2.4.3",
  "com.typesafe.akka" %% "akka-http" % "10.0.3",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.7",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
