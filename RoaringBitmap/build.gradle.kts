val deps: Map<String, String> by extra

dependencies {
    implementation(project(":shims"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("org.apache.commons:commons-lang3:${deps["commons-lang"]}")
    testImplementation("com.esotericsoftware:kryo:5.0.0-RC6")
    testImplementation("com.fasterxml.jackson.core", "jackson-databind", "2.10.3")
}

val allocationManagerTest = task<Test>("allocationManagerTest") {
    // test needs to run in its own fork to ensure registration succeeds
    filter {
        //include specific method in any of the tests
        includeTestsMatching("org.roaringbitmap.AllocationManagerTest")
    }
    useJUnitPlatform()
    failFast = true
}

tasks.test {
    dependsOn(allocationManagerTest)
    systemProperty("kryo.unsafe", "false")
    mustRunAfter(tasks.checkstyleMain)
    useJUnitPlatform()
    failFast = true
    testLogging {
        // We exclude 'passed' events
        events("skipped", "failed")
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    filter {
        // must not run this test as registration of the allocation manager will fail,
        // but if it succeeded it would cause other tests to fail
        excludeTestsMatching("org.roaringbitmap.AllocationManagerTest")
    }
}