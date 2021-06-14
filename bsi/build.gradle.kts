val deps: Map<String, String> by extra

dependencies {
    implementation(project(":RoaringBitmap"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:${deps["jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${deps["jupiter"]}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${deps["jupiter"]}")
}