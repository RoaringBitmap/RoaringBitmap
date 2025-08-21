dependencies {
    implementation(project(":roaringbitmap"))
}

tasks {
    val runAll by registering {}

    File(project.projectDir, "src/main/java").list().forEach {
        val className = it.replace(".java", "")
        val childTask = project.tasks.register("runExample$className", JavaExec::class) {
            mainClass.set(className)
            classpath = sourceSets.main.get().runtimeClasspath
            dependsOn(compileJava)
        }

        runAll.get().dependsOn(childTask)
    }
}
