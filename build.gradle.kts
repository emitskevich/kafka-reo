plugins {
    id("java")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":replicator"))
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(18))
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
