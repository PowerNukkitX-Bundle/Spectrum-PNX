plugins {
    java
}

group = "org.powernukkitx"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://www.jitpack.io")
    maven("https://repo.powernukkitx.org/releases")
    maven("https://repo.opencollab.dev/maven-releases")
    maven("https://repo.opencollab.dev/maven-snapshots")
}

dependencies {
    compileOnly(libs.powernukkitx.server)
    compileOnly(libs.bedrock.connection)
    compileOnly(libs.oshi.core)
    compileOnly(libs.okaeri.configs.core)
    compileOnly(libs.aircompressor)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
