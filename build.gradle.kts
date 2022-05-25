plugins {
    java
    `maven-publish`
}

allprojects {
    group = "science.atlarge.grademl"
    version = "0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    apply(plugin = "java")
    apply(plugin = "maven-publish")
    java.sourceCompatibility = JavaVersion.VERSION_1_8
    java.targetCompatibility = JavaVersion.VERSION_1_8

    publishing {
        publications {
            create<MavenPublication>(project.name) {
                groupId = project.group.toString()
                artifactId = project.name
                version = project.version.toString()

                from(components["java"])
            }
        }
    }
}