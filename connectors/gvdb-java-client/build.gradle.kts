import com.google.protobuf.gradle.id

plugins {
    alias(libs.plugins.protobuf)
    `maven-publish`
}

dependencies {
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.protobuf.java)
    implementation(libs.slf4j.api)
    compileOnly(libs.javax.annotation)

    testImplementation(libs.grpc.testing)
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.slf4j.simple)
}

// Copy proto from project root into build dir for codegen
val copyProto = tasks.register<Copy>("copyProto") {
    from("${rootProject.projectDir}/../proto/vectordb.proto")
    into(layout.buildDirectory.dir("extracted-protos/main/gvdb/proto"))
}

tasks.named("extractProto") {
    dependsOn(copyProto)
}

val protocVersion: String by project
val grpcVersion: String by project

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protocVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

sourceSets {
    main {
        proto {
            srcDir(layout.buildDirectory.dir("extracted-protos/main"))
        }
        resources {
            // Exclude proto files from the JAR — they're only needed for codegen
            exclude("**/*.proto")
        }
    }
}

tasks.withType<ProcessResources> {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "gvdb-java-client"
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
