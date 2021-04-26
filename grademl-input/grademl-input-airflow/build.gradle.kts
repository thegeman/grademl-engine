description = "Parsing for Airflow log files"

plugins {
    kotlin("jvm") version "1.5.0-RC"
}

dependencies {
    implementation(project(":grademl-core"))
}