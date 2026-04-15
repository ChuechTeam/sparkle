ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "sparkle"
  )

val AkkaVersion = "2.10.17"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.5.32"
)

resolvers in ThisBuild += "akka-secure-mvn" at "https://repo.akka.io/QvnBh6naQe8bYh5jOM7A-x1tDBqBx2XQgTiDMJ0q8fCRyLL6/secure"
resolvers in ThisBuild += Resolver.url("akka-secure-ivy", url("https://repo.akka.io/QvnBh6naQe8bYh5jOM7A-x1tDBqBx2XQgTiDMJ0q8fCRyLL6/secure"))(Resolver.ivyStylePatterns)