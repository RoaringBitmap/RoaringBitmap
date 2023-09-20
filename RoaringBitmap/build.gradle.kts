val deps: Map<String, String> by extra

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("org.apache.commons:commons-lang3:${deps["commons-lang"]}")
    testImplementation("com.esotericsoftware:kryo:5.0.0-RC6")
    testImplementation("com.fasterxml.jackson.core", "jackson-databind", "2.10.3")
    testImplementation("org.assertj", "assertj-core", "3.23.1")
    testImplementation("org.openjdk.jol", "jol-core", "0.16")
}

tasks.test {
    systemProperty("kryo.unsafe", "false")
    mustRunAfter(tasks.checkstyleMain)
    useJUnitPlatform()
    failFast = true

    // Define the memory requirements of tests, to prevent issues in CI while OK locally
    minHeapSize = "2G"
    maxHeapSize = "2G"

    testLogging {
        // We exclude 'passed' events
        events( "skipped", "failed")
        showStackTraces = true
        showExceptions = true
        showCauses = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        // Helps investigating OOM. But too verbose to be activated by default
        // showStandardStreams = true
    }
}
