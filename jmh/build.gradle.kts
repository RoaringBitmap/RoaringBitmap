import java.net.URI

plugins {
    id("me.champeau.gradle.jmh") version "0.4.8"
}

val deps: Map<String, String> by extra

repositories {
    maven {
        url = URI("https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local")
    }
}

dependencies {
    jmh(project(":real-roaring-dataset"))
    jmh("junit:junit:${deps["junit"]}")

    // tests and benchmarks both need dependencies: javaEWAH, extendedset, etc.
    listOf(
            project(":roaringbitmap"),
            "com.google.guava:guava:${deps["guava"]}",
            "com.googlecode.javaewah:JavaEWAH:1.0.8",
            "it.uniroma3.mat:extendedset:1.3.4",
            "com.zaxxer:SparseBitSet:1.0",
            "me.lemire.integercompression:JavaFastPFOR:0.1.11"
    ).forEach {
        jmh(it)
        testRuntime(it)
    }

    testImplementation(project(":real-roaring-dataset"))
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("junit:junit:${deps["junit"]}")

    // tests run benchmark classes, so need to depend on benchmark compile output
    testImplementation(sourceSets.jmh.get().output)
}

jmh {
    jmhVersion = "1.21"
    isIncludeTests = false
    warmupIterations = 5
    iterations = 5
    fork = 1
}

tasks.jmhJar {
    archiveBaseName.set("benchmarks")
    archiveClassifier.set("")
}

tasks.assemble {
    dependsOn(tasks.jmhJar)
}
