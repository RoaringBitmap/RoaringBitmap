val deps: Map<String, String> by extra

dependencies {
    implementation(project(":RoaringBitmap"))
    testImplementation("junit:junit:${deps["junit"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.9.9")
}

tasks.test {
    // set the property on the CLI with -P or add to gradle.properties to enable tests
    if (!project.hasProperty("roaringbitmap.fuzz-tests")) {
        exclude("**")
    }
}
