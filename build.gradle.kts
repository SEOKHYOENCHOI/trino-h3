plugins {
    java
    jacoco
    id("com.gradleup.shadow") version "9.0.0"
    id("com.diffplug.spotless") version "6.25.0"
}

// JaCoCo 0.8.13 for JDK 24 support
jacoco {
    toolVersion = "0.8.13"
}

// Read versions from gradle.properties
val trinoVersion: String by project
val h3Version: String by project
val jtsVersion: String by project
val sliceVersion: String by project
val junitVersion: String by project
val assertjVersion: String by project
val googleJavaFormatVersion: String by project

group = project.property("group") as String

// Use git tag version if available (CI), otherwise use gradle.properties
version = System.getenv("RELEASE_VERSION")
    ?: project.property("version") as String

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // H3 Core Library
    implementation("com.uber:h3:$h3Version")

    // JTS Core for geometric operations
    implementation("org.locationtech.jts:jts-core:$jtsVersion")

    // Trino SPI Dependencies (provided) - Version 476 (requires JDK 24)
    compileOnly("io.trino:trino-spi:$trinoVersion")
    compileOnly("io.trino:trino-main:$trinoVersion")
    compileOnly("io.trino:trino-geospatial-toolkit:$trinoVersion")
    compileOnly("io.trino:trino-plugin-toolkit:$trinoVersion")
    compileOnly("io.airlift:slice:$sliceVersion")

    // Annotation Processing
    annotationProcessor("io.trino:trino-spi:$trinoVersion")

    // Test Dependencies
    testImplementation("io.trino:trino-testing:$trinoVersion")
    testImplementation("io.trino:trino-geospatial-toolkit:$trinoVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Force upgrade vulnerable transitive dependencies
// These overrides apply only to test scope since compileOnly dependencies
// are provided by Trino server at runtime
configurations.all {
    resolutionStrategy {
        force(
            // Fix CVE-2024-36114 (aircompressor 0.25 -> 0.27)
            "io.airlift:aircompressor:0.27",
            // Fix CVE-2025-11226, CVE-2024-12798, CVE-2024-12801 (logback-core 1.4.8 -> 1.5.16)
            "ch.qos.logback:logback-core:1.5.16",
            "ch.qos.logback:logback-classic:1.5.16",
            // Fix CVE-2024-25710, CVE-2024-26308 (commons-compress 1.24.0 -> 1.27.1)
            "org.apache.commons:commons-compress:1.27.1",
            // Fix CVE-2024-29857 and others (bcprov 1.76 -> 1.79)
            "org.bouncycastle:bcprov-jdk18on:1.79"
        )
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = false
    }
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.shadowJar {
    archiveClassifier.set("")
    archiveBaseName.set("trino-h3")

    // Relocate dependencies to avoid conflicts
    relocate("org.locationtech.jts", "io.shchoi.trino.h3.shaded.org.locationtech.jts")
   // relocate("com.uber.h3core", "io.shchoi.trino.h3.shaded.com.uber.h3core")

    // Exclude signatures and other metadata
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")

    // Keep only necessary files
    minimize {
        exclude(dependency("com.uber:h3:.*"))
        exclude(dependency("org.locationtech.jts:jts-core:.*"))
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

// Spotless configuration for code formatting
spotless {
    java {
        googleJavaFormat(googleJavaFormatVersion)
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()

        // Custom formatting rules
        indentWithSpaces(2)

        target("src/**/*.java")
        targetExclude("build/**")
    }
}

// Task to display project information
tasks.register("projectInfo") {
    doLast {
        println("Project: ${project.name}")
        println("Group: ${project.group}")
        println("Version: ${project.version}")
        println("Java Version: ${java.sourceCompatibility}")
    }
}

// Task to clean generated files
tasks.clean {
    delete("out")
    delete(".gradle")
}
