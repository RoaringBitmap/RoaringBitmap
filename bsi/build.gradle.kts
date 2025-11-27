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
    testLogging {
        // We exclude 'passed' events
        events( "skipped", "failed")
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
