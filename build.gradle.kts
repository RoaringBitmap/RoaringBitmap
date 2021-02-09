// https://jeroenmols.com/blog/2021/02/04/migratingjcenter/
// import com.jfrog.bintray.gradle.BintrayExtension

// This is a kotlin gradle file
// https://outadoc.fr/2020/06/converting-gradle-to-gradle-kts/

plugins {
    // id("org.jetbrains.kotlin.jvm") version "1.2.41"

    id("net.researchgate.release") version "2.8.0"
    // https://jeroenmols.com/blog/2021/02/04/migratingjcenter/
    // id("com.jfrog.bintray") version "1.8.4" apply false
    id("com.github.kt3k.coveralls") version "2.8.4" apply false
    // https://github.com/rwinch/gradle-publish-ossrh-sample#apply-plugins
    //id("java")
    id("signing")

    // https://issues.sonatype.org/browse/OSSRH-55639
    // This plugin is applied by 'de.marcphilipp.nexus-publish'
    // `maven-publish`

    // https://central.sonatype.org/pages/gradle.html
    // 'maven' looks outdated compared to 'maven-publish'
    // `maven`

    // https://github.com/Codearte/gradle-nexus-staging-plugin/
    id("io.codearte.nexus-staging") version "0.22.0"
    id("de.marcphilipp.nexus-publish") version "0.4.0"
}

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
        // https://jeroenmols.com/blog/2021/02/04/migratingjcenter/
        // jcenter()
        mavenCentral()
    }

    tasks.withType<JavaCompile> {
        options.isDeprecation = true
        options.isWarnings = true
        if (JavaVersion.current().isJava9Compatible) {
          options.compilerArgs = listOf("--release", "8", "-Xlint:unchecked")
        }
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
        apply(plugin = "signing")

        // https://jeroenmols.com/blog/2021/02/04/migratingjcenter/
        // apply(plugin = "com.jfrog.bintray")

        // Is this an alternative to the following?
        // java {
        //     withJavadocJar()
        //     withSourcesJar()
        //}
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

        // https://docs.gradle.org/current/userguide/publishing_maven.html#publishing_maven
        publishing {
            publications {
                create<MavenPublication>("mavenJava") {
                    groupId = project.group.toString()
                    artifactId = project.name
                    version = project.version.toString()

                    from(components["java"])
                    artifact(tasks["sourceJar"])
                    artifact(tasks["docJar"])

                    // https://issues.sonatype.org/browse/OSSRH-55639
                    // Looks not useful given nexusStaging and nexusPublishing at the bottom
                    // repositories {
                    //     maven {
                    //         credentials {
                    //             username = project.property("ossrhUsername").toString()
                    //             password = project.property("ossrhPassword").toString()
                    //         }
                    //         val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                    //         val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
                    //         url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
                    //     }
                    // }

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
        }

        // This should enable signing only on RELEASE versions, and if local environment has a signingKeyId
        // https://docs.gradle.org/current/userguide/signing_plugin.html
        tasks.withType<Sign>().configureEach {
            onlyIf { project.extra["isReleaseVersion"] as Boolean && project.hasProperty("signing.keyId") }
        }

// https://docs.gradle.org/current/userguide/signing_plugin.html#sec:signatory_credentials
// May be set in ${user.home}/gradle.properties
// https://stackoverflow.com/questions/28289172/gradle-gradle-properties-file-not-being-read
// export USER_HOME=$HOME
// ./gradlew properties
// See 'gpg --list-signatures' to get a 16-chars keyId
// https://github.com/gradle/gradle/issues/1918
// -> 16-chars is not a valid key
// -> Take the last 8 characters of the 16-chars keyId (LAWL)
// Or 'gpg2 --list-keys --keyid-format SHORT'
// signing.keyId=YourKeyId
// BenoitLacelle: I did not find how to input this in a interactive way
// Except: '-Psigning.password=secret'
// signing.password=YourPublicKeyPassword
// You may need to to: 'gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg'
// signing.secretKeyRingFile=PathToYourKeyRingFile
// ./gradlew signArchives -Psigning.secretKeyRingFile=/Users/blacelle/.gnupg/secring.gpg -Psigning.password=secret -Psigning.keyId=0x9502E68A
        signing {
            isRequired = true

            // sign(configurations.archives.get())
            sign(publishing.publications)
        }
    }
}

release {
    // for some odd reason, we used to have our tags be of the form RoaringBitmap-0.1.0
    // instead of just 0.1.0 or v0.1.0.
    tagTemplate = "\$version"
}

// https://github.com/rwinch/gradle-publish-ossrh-sample#configure-nexus-staging-plugin
nexusStaging {
    if (project.hasProperty("ossrhUsername")) {
        // https://stackoverflow.com/questions/58820891/how-to-define-gradle-project-properties-with-kotlin-dsl
        username = project.extra["ossrhUsername"].toString()
    }
    if (project.hasProperty("ossrhPassword")) {
        password = project.extra["ossrhPassword"].toString()
    }
    repositoryDescription = "Release ${project.group} ${project.version}"
}

// https://github.com/rwinch/gradle-publish-ossrh-sample#configure-nexus-publishing-plugin
// ./gradlew publishToSonatype
nexusPublishing {
    repositories {
        sonatype {
            if (project.hasProperty("ossrhUsername")) {
                username.set(project.extra["ossrhUsername"].toString())
            }
            if (project.hasProperty("ossrhPassword")) {
                password.set(project.extra["ossrhPassword"].toString())
            }
        }
    }
}

// ./gradlew publishMavenPublicationToLocalRepository
// ./gradlew publishToMavenLocal
//publishing {
//    repositories {
//        maven {
//            name = "local"
            // change URLs to point to your repos, e.g. http://my.org/repo
//            val releasesRepoUrl = "$buildDir/repos/releases"
//            val snapshotsRepoUrl = "$buildDir/repos/snapshots"
//            if (version.toString().endsWith("SNAPSHOT")) {
//                url = uri(snapshotsRepoUrl)
//            } else {
//                url = uri(releasesRepoUrl)
//            }
//        }
//    }
//}