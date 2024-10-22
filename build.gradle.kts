import java.net.URI
import java.time.Duration

plugins {
    id("net.researchgate.release") version "2.8.1"
    id("com.github.ben-manes.versions") version "0.38.0"
    id("maven-publish")
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/RoaringBitmap/RoaringBitmap")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}

// some parts of the Kotlin DSL don't work inside a `subprojects` block yet, so we do them the old way
// (without typesafe accessors)

subprojects {
    // used in per-subproject dependencies
    @Suppress("UNUSED_VARIABLE") val deps by extra {
        mapOf(
                "jupiter" to "5.6.1",
                "guava" to "20.0",
                "commons-lang" to "3.4"
        )
    }

    apply(plugin = "java-library")

    repositories {
        mavenCentral()
    }

    group = "org.roaringbitmap"

    tasks {
        withType<JavaCompile> {
            options.isDeprecation = true
            options.isWarnings = true
            options.compilerArgs = listOf("-Xlint:unchecked")
            options.release.set(8)
        }

        withType<Javadoc> {
            options {
                // suppress javadoc's complaints about undocumented things
                // we have to set a dummy "value" (here, `true`) to have the option actually used
                (this as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none").value = true
            }
        }

        withType<Test> {
            val javaToolchains  = project.extensions.getByType<JavaToolchainService>()
            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(
                    JavaLanguageVersion.of((project.properties["testOnJava"] ?: "11").toString()))
            })
        }
    }
}

subprojects.filter { !listOf("jmh", "fuzz-tests", "examples", "bsi", "simplebenchmark").contains(it.name) }.forEach {
    it.run {
        apply(plugin = "checkstyle")

        tasks {
            withType<Checkstyle> {
                configFile = File(rootProject.projectDir, "RoaringBitmap/style/roaring_google_checks.xml")
                isIgnoreFailures = false
                isShowViolations = true

                // Skip checkstyle on module-info.java since it breaks.
                exclude("module-info.java")
            }

            // don't checkstyle source
            named<Checkstyle>("checkstyleTest") {
                exclude("**/**")
            }
        }
    }
}


tasks {
    register("build") {
        // dummy build task to appease release plugin
    }
}

release {
    // for some odd reason, we used to have our tags be of the form RoaringBitmap-0.1.0
    // instead of just 0.1.0 or v0.1.0.
    tagTemplate = "\$version"
}
