description = "Query engine for GradeML"

plugins {
    kotlin("jvm") version "1.5.0"
    application
}

application {
    mainClass.set("science.atlarge.grademl.query.QueryCli")
}

dependencies {
    implementation(project(":grademl-core"))
    implementation(project(":grademl-input:grademl-input-airflow"))
    implementation(project(":grademl-input:grademl-input-resource-monitor"))
    implementation(project(":grademl-input:grademl-input-spark"))
    implementation(project(":grademl-input:grademl-input-tensorflow"))

    implementation("org.jline:jline:3.19.0")
    runtimeOnly("org.fusesource.jansi:jansi:2.3.2")

    api("com.github.h0tk3y.betterParse:better-parse:0.4.2")
    implementation("it.unimi.dsi:fastutil-core:8.5.6")

    testImplementation("org.junit.jupiter:junit-jupiter:5.8.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}