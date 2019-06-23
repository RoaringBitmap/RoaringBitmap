val deps: Map<String, String> by extra

dependencies {
    implementation(project(":shims"))

    testImplementation("junit:junit:${deps["junit"]}")
    testImplementation("com.google.guava:guava:${deps["guava"]}")
    testImplementation("org.apache.commons:commons-lang3:${deps["commons-lang"]}")
    testImplementation("com.esotericsoftware:kryo:5.0.0-RC1")
}


tasks.test {
    mustRunAfter(tasks.checkstyleMain)
    useJUnit()
    failFast = true
    maxParallelForks = 8
}