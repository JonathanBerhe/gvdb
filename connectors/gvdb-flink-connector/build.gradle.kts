plugins {
    alias(libs.plugins.shadow)
    `maven-publish`
}

dependencies {
    implementation(project(":gvdb-java-client"))

    compileOnly(libs.flink.streaming)

    testImplementation(libs.flink.streaming)
    testImplementation(libs.flink.test.utils)
    testImplementation(libs.flink.runtime.web)
    testImplementation(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveClassifier.set("all")

    relocate("io.grpc", "io.gvdb.shaded.io.grpc")
    relocate("com.google.protobuf", "io.gvdb.shaded.com.google.protobuf")
    relocate("com.google.common", "io.gvdb.shaded.com.google.common")
    relocate("com.google.thirdparty", "io.gvdb.shaded.com.google.thirdparty")
    relocate("com.google.api", "io.gvdb.shaded.com.google.api")
    relocate("com.google.rpc", "io.gvdb.shaded.com.google.rpc")
    relocate("com.google.gson", "io.gvdb.shaded.com.google.gson")
    relocate("com.google.errorprone", "io.gvdb.shaded.com.google.errorprone")
    relocate("javax.annotation", "io.gvdb.shaded.javax.annotation")
    relocate("org.checkerframework", "io.gvdb.shaded.org.checkerframework")
    relocate("org.codehaus", "io.gvdb.shaded.org.codehaus")
    relocate("io.perfmark", "io.gvdb.shaded.io.perfmark")

    minimize {
        exclude(dependency("io.grpc:grpc-netty-shaded:.*"))
    }

    dependencies {
        exclude(dependency("org.apache.flink:.*"))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "gvdb-flink-connector"
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/JonathanBerhe/gvdb")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
