import java.net.URI

plugins {
    id("me.champeau.gradle.jmh") version "0.4.8"
    id("com.github.johnrengelman.shadow") version "5.0.0"
}

val deps: Map<String, String> by extra

repositories {
    maven {
        url = URI("https://metamx.artifactoryonline.com/artifactory/pub-libs-releases-local")
    }
}

dependencies {
    jmh(project(":real-roaring-dataset"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")

    // tests and benchmarks both need dependencies: javaEWAH, extendedset, etc.
    listOf(
            project(":RoaringBitmap"),
            "com.google.guava:guava:${deps["guava"]}",
            "com.googlecode.javaewah:JavaEWAH:1.0.8",
            "it.uniroma3.mat:extendedset:1.3.4",
            "com.zaxxer:SparseBitSet:1.0",
            "me.lemire.integercompression:JavaFastPFOR:0.1.11"
    ).forEach {
        jmh(it)
        testRuntimeOnly(it)
    }

    testImplementation(project(":real-roaring-dataset"))
    testImplementation("com.google.guava:guava:${deps["guava"]}")

    // tests run benchmark classes, so need to depend on benchmark compile output
    testImplementation(sourceSets.jmh.get().output)
}

jmh {
    jmhVersion = "1.23"
    // tests depend on jmh, not the other way around
    isIncludeTests = false
    warmupIterations = 5
    iterations = 5
    fork = 1
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
}

tasks.test {
    // set the property on the CLI with -P or add to gradle.properties to enable tests
    if (!project.hasProperty("roaringbitmap.jmh")) {
        exclude("**")
    } else {
        // stop these tests from running before RoaringBitmap
        shouldRunAfter(project(":RoaringBitmap").tasks.test)
        useJUnitPlatform()
        failFast = true
    }
}


// jmhJar task provided by jmh gradle plugin is currently broken
// https://github.com/melix/jmh-gradle-plugin/issues/97
// so instead, we configure the shadowJar task to have JMH bits in it
tasks.shadowJar {
    archiveBaseName.set("benchmarks")
    archiveVersion.set("")
    archiveClassifier.set("")

    manifest {
        attributes(Pair("Main-Class", "org.openjdk.jmh.Main"))
        attributes(Pair("Multi-Release", "true"))
    }

    // include dependencies
    configurations.add(project.configurations.jmh.get())
    // include benchmark classes
    from(project.sourceSets.jmh.get().output)
    // include generated java source, BenchmarkList and other JMH resources
    from(tasks.jmhRunBytecodeGenerator.get().outputs)
    // include compiled generated classes
    from(tasks.jmhCompileGeneratedClasses.get().outputs)

    dependsOn(tasks.jmhCompileGeneratedClasses)
}