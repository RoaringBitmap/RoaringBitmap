val deps: Map<String, String> by extra

dependencies {
    implementation(project(":roaringbitmap"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")
}


tasks.test {
    systemProperty("kryo.unsafe", "false")
    useJUnitPlatform()
    failFast = true

    // Define the memory requirements of tests, to prevent issues in CI while OK locally
    minHeapSize = "2G"
    maxHeapSize = "2G"

    testLogging {
        // We exclude 'passed' events
        events( "skipped", "failed")
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
