plugins {
    id("net.researchgate.release") version "2.8.1"
    id("com.github.ben-manes.versions") version "0.38.0"
    id("maven-publish")
    id("com.diffplug.spotless") version "6.25.0"
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

    apply(plugin = "com.diffplug.spotless")

    // You can format the codebase with `./gradlew spotlessApply`
    // You check the codebase format with `./gradlew spotlessCheck`
    spotless {
        // Ratchetting from master means we check/apply only files which are changed relatively to master
        // This is especially useful for performance, given the whole codebase has been formatted with Spotless.
        ratchetFrom("origin/master")

        java {
            // Disbale javadoc formatting as most the javacode do not follow HTML syntax.
            googleJavaFormat().reflowLongStrings().formatJavadoc(false)
            formatAnnotations()

            importOrder("\\#", "org.roaringbitmap", "", "java", "javax")
            removeUnusedImports()

            trimTrailingWhitespace()
            endWithNewline()

            // https://github.com/opensearch-project/opensearch-java/commit/2d6d5f86a8db9c7c9e7b8d0f54df97246f7b7d7e
            // https://github.com/diffplug/spotless/issues/649
            val wildcardImportRegex = Regex("""^import\s+(?:static\s+)?[^*\s]+\.\*;$""", RegexOption.MULTILINE)
            custom("Refuse wildcard imports") { contents ->
                // Wildcard imports can't be resolved by spotless itself.
                // This will require the developer themselves to adhere to best practices.
                val wildcardImports = wildcardImportRegex.findAll(contents)
                if (wildcardImports.any()) {
                    var msg = """
                    Please replace the following wildcard imports with explicit imports ('spotlessApply' cannot resolve this issue):
                """.trimIndent()
                    wildcardImports.forEach {
                        msg += "\n\t- ${it.value}"
                    }
                    msg += "\n"
                    throw AssertionError(msg)
                }
                contents
            }
        }
    }
}

subprojects.filter { listOf("roaringbitmap", "bsi").contains(it.name) }.forEach { project ->
    project.run {
        apply(plugin = "maven-publish")
        configure<JavaPluginExtension> {
            withSourcesJar()
            withJavadocJar()
        }

        configure<PublishingExtension> {
            publications {
                register<MavenPublication>("sonatype") {
                    groupId = project.group.toString()
                    artifactId = project.name
                    version = project.version.toString()

                    from(components["java"])

                    // requirements for maven central
                    // https://central.sonatype.org/pages/requirements.html
                    pom {
                        name.set("${project.group}:${project.name}")
                        description.set("Roaring bitmaps are compressed bitmaps (also called bitsets) which tend to outperform conventional compressed bitmaps such as WAH or Concise.")
                        url.set("https://github.com/RoaringBitmap/RoaringBitmap")
                        issueManagement {
                            system.set("GitHub Issue Tracking")
                            url.set("https://github.com/RoaringBitmap/RoaringBitmap/issues")
                        }
                        licenses {
                            license {
                                name.set("Apache 2")
                                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                                distribution.set("repo")
                            }
                        }
                        developers {
                            developer {
                                id.set("lemire")
                                name.set("Daniel Lemire")
                                email.set("lemire@gmail.com")
                                url.set("http://lemire.me/en/")
                                roles.addAll("architect", "developer", "maintainer")
                                timezone.set("-5")
                                properties.put("picUrl", "http://lemire.me/fr/images/JPG/profile2011B_152.jpg")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://github.com/RoaringBitmap/RoaringBitmap.git")
                            developerConnection.set("scm:git:https://github.com/RoaringBitmap/RoaringBitmap.git")
                            url.set("https://github.com/RoaringBitmap/RoaringBitmap")
                        }
                    }
                }
            }

             // A safe throw-away place to publish to:
            // ./gradlew publishSonatypePublicationToLocalDebugRepository -Pversion=foo
            repositories {
                maven {
                    name = "localDebug"
                    url = project.buildDir.toPath().resolve("repos").resolve("localDebug").toUri()
                }
            }

            // ./gradlew publishSonatypePublicationToGitHubPackagesRepository
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


    }
}

release {
    // for some odd reason, we used to have our tags be of the form roaringbitmap-0.1.0
    // instead of just 0.1.0 or v0.1.0.
    tagTemplate = "\$version"
}
	
