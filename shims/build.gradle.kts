// See https://blog.gradle.org/mrjars and https://github.com/melix/mrjar-gradle for more on multi release jars

sourceSets {
    create("java11") {
        java {
            srcDir("src/java11/main")
        }
    }
}

tasks.named<JavaCompile>("compileJava11Java") {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

tasks.named<Jar>("jar") {
    into("META-INF/versions/11") {
        from(sourceSets.named("java11").get().output)
    }
    manifest.attributes(
            Pair("Multi-Release", "true")
    )
}
