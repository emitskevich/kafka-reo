import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("java")
    id("application")
    id("com.google.protobuf") version "0.8.19"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.4.5")
    implementation("org.jetbrains:annotations:23.1.0")
    implementation("com.github.pcj:google-options:1.0.0")
    implementation("org.yaml:snakeyaml:1.33")
    implementation("com.google.protobuf:protobuf-java:3.21.12")
    implementation("org.apache.kafka:kafka-streams:3.3.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testImplementation("org.mockito:mockito-core:4.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:4.11.0")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.3.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

application {
    mainClass.set("com.github.emitskevich.Launcher")
}

tasks {
    protobuf {
        protoc { artifact = "com.google.protobuf:protoc:3.21.12" }
    }
}

sourceSets {
    main {
        java {
            srcDir("${buildDir.absolutePath}/generated")
        }
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
