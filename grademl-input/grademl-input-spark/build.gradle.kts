description = "Parsing for Spark log files"

plugins {
    kotlin("jvm") version "1.5.0"
}

dependencies {
    implementation(project(":grademl-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.2.0")
}