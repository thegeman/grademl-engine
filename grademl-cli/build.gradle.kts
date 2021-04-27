description = "CLI for GradeML"

plugins {
    kotlin("jvm") version "1.5.0-RC"
}

dependencies {
    implementation(project(":grademl-core"))
    implementation(project(":grademl-input:grademl-input-airflow"))
    implementation(project(":grademl-input:grademl-input-resource-monitor"))
}