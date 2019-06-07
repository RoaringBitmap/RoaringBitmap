import com.jfrog.bintray.gradle.BintrayExtension

plugins {
    id("net.researchgate.release") version "2.8.0"
    id("com.jfrog.bintray") version "1.8.4" apply false
    id("com.github.kt3k.coveralls") version "2.8.4" apply false
}

// some parts of the Kotlin DSL don't work inside a `subprojects` block yet, so we do them the old way
// (without typesafe accessors)

subprojects {
    // used in per-subproject dependencies
    @Suppress("UNUSED_VARIABLE") val deps by extra {
        mapOf(
                "junit" to "4.12",
                "guava" to "20.0",
                "commons-lang" to "3.4"
        )
    }

    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.github.kt3k.coveralls")

    repositories {
        jcenter()
    }

    tasks.withType<JavaCompile> {
        options.isDeprecation = true
        options.isWarnings = true
    }

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
        group = "org.roaringbitmap"
    }

    tasks.named<JacocoReport>("jacocoTestReport") {
        reports {
            // used by coveralls
            xml.isEnabled = true
        }
    }
}

subprojects.filter { !listOf("jmh", "fuzz-tests", "examples", "simplebenchmark").contains(it.name) }.forEach {
    it.run {
        apply(plugin = "checkstyle")

        tasks.withType<Checkstyle> {
            configFile = File(rootProject.projectDir, "RoaringBitmap/style/roaring_google_checks.xml")
            isIgnoreFailures = false
            isShowViolations = true
        }

        // don't checkstyle source
        tasks.named<Checkstyle>("checkstyleTest") {
            exclude("**/**")
        }
    }
}

subprojects.filter { listOf("RoaringBitmap", "shims").contains(it.name) }.forEach { project ->
    project.run {
        apply(plugin = "maven-publish")
        apply(plugin = "com.jfrog.bintray")

        tasks {
            register<Jar>("sourceJar") {
                from(project.the<SourceSetContainer>()["main"].allJava)
                archiveClassifier.set("sources")
            }

            register<Jar>("docJar") {
                from(project.tasks["javadoc"])
                archiveClassifier.set("javadoc")
            }
        }

        configure<PublishingExtension>() {
            publications {
                register<MavenPublication>("bintray") {
                    groupId = project.group.toString()
                    artifactId = project.name
                    version = project.version.toString()

                    from(components["java"])
                    artifact(tasks["sourceJar"])
                    artifact(tasks["docJar"])
                }
            }
        }

        configure<BintrayExtension> {
            user = rootProject.findProperty("bintrayUser")?.toString()
            key = rootProject.findProperty("bintrayApiKey")?.toString()
            setPublications("bintray")

            with(pkg) {
                repo = "maven"
                setLicenses("Apache-2.0")
                vcsUrl = "https://github.com/RoaringBitmap/RoaringBitmap"
                name = "RoaringBitmap"
                userOrg = "roaringbitmap"

                with(version) {
                    name = project.version.toString()
                    released = java.util.Date().toString()
                    vcsTag = "RoaringBitmap-${project.version}"
                }
            }
        }
    }
}

tasks {
    create("build") {
        // dummy build task to appease release plugin
    }
}

release {
    tagTemplate = "RoaringBitmap-\$version"
}

tasks.afterReleaseBuild {
    dependsOn(tasks.named("bintrayUpload"))
}
