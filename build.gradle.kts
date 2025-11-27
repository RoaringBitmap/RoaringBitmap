plugins {
    id("net.researchgate.release") version "2.8.1"
    id("com.github.ben-manes.versions") version "0.38.0"
    id("maven-publish")
    id("signing")
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
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
            val javaToolchains = project.extensions.getByType<JavaToolchainService>()
            val requestedVersion = (project.properties["testOnJava"] ?: "11").toString().toInt()
            val currentVersion = JavaVersion.current().majorVersion.toInt()
            val versionToUse = if (currentVersion > requestedVersion) currentVersion else requestedVersion
            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(versionToUse))
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
                    url = project.layout.buildDirectory.dir("repos/localDebug").get().asFile.toURI()
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

            // ./gradlew publishSonatypePublicationToSonatypeRepository
            repositories {
                maven {
                    url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                    credentials {
                        username = System.getenv("MAVEN_USER")
                        password = System.getenv("MAVEN_PASSWORD")
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

nexusPublishing {
    repositories {
        sonatype()
    }
}

// Validate Sonatype and GPG secrets if a Sonatype publish/close/release is requested.
run {
    val publishRequested = gradle.startParameter.taskNames.any { t ->
        listOf("publishToSonatype", "publishToSonatypeRepository", "closeAndReleaseSonatypeStagingRepository", "closeSonatypeStagingRepository", "releaseSonatypeStagingRepository").any { it in t }
    }
    if (publishRequested) {
        val user = System.getenv("MAVEN_USER")
        val pass = System.getenv("MAVEN_PASSWORD")
        if (user.isNullOrBlank() || pass.isNullOrBlank()) {
            throw org.gradle.api.GradleException("Missing Sonatype credentials. Set secrets MAVEN_USER/MAVEN_PASSWORD.")
        }
        val gpgKey = System.getenv("GPG_PRIVATE_KEY")
        val gpgPass = System.getenv("GPG_PASSPHRASE")
        if (gpgKey.isNullOrBlank() || gpgPass.isNullOrBlank()) {
            throw org.gradle.api.GradleException("Missing GPG_PRIVATE_KEY or GPG_PASSPHRASE. These are required to sign artifacts before publishing to Sonatype.")
        }
    }
}

// Root-level signing configuration: use in-memory PGP key from CI to sign subproject publications
configure<org.gradle.plugins.signing.SigningExtension> {
    val signingKey = System.getenv("GPG_PRIVATE_KEY")
    val signingPassphrase = System.getenv("GPG_PASSPHRASE")
    if (!signingKey.isNullOrBlank()) {
        useInMemoryPgpKeys(signingKey, signingPassphrase)
        subprojects.filter { listOf("roaringbitmap", "bsi").contains(it.name) }.forEach { proj ->
            val pub = proj.extensions.findByType(PublishingExtension::class)?.publications?.findByName("sonatype")
            if (pub != null) {
                sign(pub)
            }
        }
    }
}
