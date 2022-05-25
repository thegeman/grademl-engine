description = "Jupyter integration for GradeML"

plugins {
    kotlin("jvm") version "1.5.0"
}

dependencies {
    implementation(project(":grademl-core"))
    implementation(project(":grademl-input:grademl-input-airflow"))
    implementation(project(":grademl-input:grademl-input-resource-monitor"))
    implementation(project(":grademl-input:grademl-input-spark"))
    implementation(project(":grademl-input:grademl-input-tensorflow"))
    implementation(project(":grademl-query"))

    implementation("org.jetbrains.lets-plot:lets-plot-common:2.3.0")
    implementation("org.jetbrains.lets-plot:lets-plot-kotlin-jvm:3.2.0")
}