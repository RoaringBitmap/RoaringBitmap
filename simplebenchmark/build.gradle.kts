dependencies {
    implementation(project(":roaringbitmap"))
}

tasks {
    val runAll by registering {}

    val rootDir = File(rootProject.projectDir, "/real-roaring-dataset/src/main/resources/real-roaring-dataset/")
    rootDir.list()
            .sorted()
            .filter { it.endsWith(".zip") }
            .forEach { zipFile ->
                val niceName = zipFile.replace(".zip", "")
                val childTask = project.tasks.create("runBenchmark${niceName.capitalize()}", JavaExec::class) {
                    main = "simplebenchmark"
                    classpath = sourceSets.main.get().runtimeClasspath
                    dependsOn(compileJava)
                    args = listOf(File(rootDir, zipFile).toString())
                }

                runAll.get().dependsOn(childTask)
            }
}
