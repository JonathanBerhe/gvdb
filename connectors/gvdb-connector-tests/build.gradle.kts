dependencies {
    testImplementation(project(":gvdb-java-client"))
    testImplementation(project(":gvdb-spark-connector"))
    testImplementation(project(":gvdb-flink-connector"))

    testImplementation(libs.spark.sql)
    testImplementation(libs.flink.streaming)
    testImplementation(libs.flink.test.utils)
    testImplementation(libs.flink.runtime.web)
    testImplementation(libs.slf4j.simple)
}
