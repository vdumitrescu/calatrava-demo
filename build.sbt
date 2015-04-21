name := "calatrava-demo-client"

scalaVersion in ThisBuild := "2.11.6"

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.0.0",
  "com.amazonaws" % "aws-java-sdk" % "1.9.13",
  "com.typesafe.play" %% "play-json" % "2.3.7"
)

