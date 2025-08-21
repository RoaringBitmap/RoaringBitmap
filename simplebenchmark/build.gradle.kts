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
                val childTask = project.tasks.register("runBenchmark${niceName.replaceFirstChar { it.uppercase() }}", JavaExec::class) {
                    mainClass.set("simplebenchmark")
                    classpath = sourceSets.main.get().runtimeClasspath
                    dependsOn(compileJava)
                    args = listOf(File(rootDir, zipFile).toString())
                }

                runAll.get().dependsOn(childTask)
            }
}
