description = "Parsing for Airflow log files"

plugins {
    kotlin("jvm") version "1.5.0"
}

dependencies {
    implementation(project(":grademl-core"))
}