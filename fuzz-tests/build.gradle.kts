val deps: Map<String, String> by extra

dependencies {
    implementation(project(":roaringbitmap"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("com.fasterxml.jackson.core", "jackson-databind", "2.10.3")
}

tasks.test {
    // set the property on the CLI with -P or add to gradle.properties to enable tests
    if (!project.hasProperty("roaringbitmap.fuzz-tests")) {
       exclude("**")
    }
    failFast = true
}
