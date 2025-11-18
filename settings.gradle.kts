rootProject.name = "trino-h3"

// Enable Gradle build cache
buildCache {
    local {
        isEnabled = true
        directory = File(rootDir, ".gradle/build-cache")
    }
}

// Configure plugin management
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}
