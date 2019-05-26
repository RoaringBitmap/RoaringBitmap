val deps: Map<String, String> by extra

dependencies {
    implementation(project(":roaringbitmap"))
    testImplementation("junit:junit:${deps["junit"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.9.9")
}
