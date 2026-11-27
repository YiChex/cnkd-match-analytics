val cnkdApiJar = providers.gradleProperty("cnkdApiJar")
    .map { rootProject.file(it) }
    .orElse(rootProject.layout.projectDirectory.file("sdk/server-plugin-api-1.3.0.jar").asFile)
check(cnkdApiJar.get().isFile) {
    "Missing SDK. Restore sdk/server-plugin-api-1.3.0.jar or pass -PcnkdApiJar=<API 1.3.0 jar>"
}
dependencies {
 implementation(project(":contracts"))
 compileOnly(files(cnkdApiJar))
 testImplementation(files(cnkdApiJar))
}
tasks.jar {
 dependsOn(configurations.runtimeClasspath)
 archiveFileName.set("cnkd-match-analytics-0.1.0.jar")
 duplicatesStrategy=DuplicatesStrategy.EXCLUDE
 from(configurations.runtimeClasspath.get().map { if(it.isDirectory) it else zipTree(it) })
 exclude("META-INF/*.SF","META-INF/*.RSA","META-INF/*.DSA","module-info.class","META-INF/versions/**")
}
