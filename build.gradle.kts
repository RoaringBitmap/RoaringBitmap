import java.net.URI
import java.time.Duration

plugins {
    id("net.researchgate.release") version "2.8.1"
    id("io.github.gradle-nexus.publish-plugin") version "1.0.0"
    id("com.github.kt3k.coveralls") version "2.8.4" apply false
    id("com.github.ben-manes.versions") version "0.38.0"
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
    apply(plugin = "jacoco")
    apply(plugin = "com.github.kt3k.coveralls")

    repositories {
        mavenCentral()
    }

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    group = "org.roaringbitmap"

    tasks {
        withType<JavaCompile> {
            options.isDeprecation = true
            options.isWarnings = true
            if (JavaVersion.current().isJava9Compatible) {
                options.compilerArgs = listOf("--release", "8", "-Xlint:unchecked")
            }
        }

        named<JacocoReport>("jacocoTestReport") {
            reports {
                // used by coveralls
                xml.isEnabled = true
            }
        }

        withType<Javadoc> {
            options {
                // suppress javadoc's complaints about undocumented things
                // we have to set a dummy "value" (here, `true`) to have the option actually used
                (this as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none").value = true
            }
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
            }

            // don't checkstyle source
            named<Checkstyle>("checkstyleTest") {
                exclude("**/**")
            }
        }
    }
}

subprojects.filter { listOf("RoaringBitmap", "shims", "bsi").contains(it.name) }.forEach { project ->
    project.run {
        apply(plugin = "maven-publish")
        apply(plugin = "signing")

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
                    url = URI.create("file:///${project.buildDir}/repos/localDebug")
                }
            }
        }

        // don't barf for devs without signing set up
        if (project.hasProperty("signing.keyId")) {
            configure<SigningExtension> {
                sign(project.extensions.getByType<PublishingExtension>().publications["sonatype"])
            }
        }

        // releasing should publish
        rootProject.tasks.afterReleaseBuild {
            dependsOn(provider { project.tasks.named("publishToSonatype") })
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

nexusPublishing {
    repositories {
        sonatype {
            // sonatypeUsername and sonatypePassword properties are used automatically
            // id found via clicking the desired profile in the web ui and noting the url fragment
            stagingProfileId.set("144dd9b55bb0c2")
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
        }
    }
    // these are not strictly required. The default timeouts are set to 1 minute. But Sonatype can be really slow.
    // If you get the error "java.net.SocketTimeoutException: timeout", these lines will help.
    connectTimeout.set(Duration.ofMinutes(3))
    clientTimeout.set(Duration.ofMinutes(3))
}
