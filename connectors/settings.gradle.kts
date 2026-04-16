pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "gvdb-connectors"

include("gvdb-java-client")
include("gvdb-spark-connector")
include("gvdb-flink-connector")
include("gvdb-connector-tests")

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
