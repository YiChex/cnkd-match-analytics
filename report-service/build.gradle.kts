dependencies {
 implementation(project(":contracts"))
 implementation("org.xerial:sqlite-jdbc:3.47.2.0")
 runtimeOnly("org.slf4j:slf4j-nop:2.0.16")
}
tasks.jar {
 dependsOn(configurations.runtimeClasspath)
 archiveFileName.set("cnkd-analytics-service-0.1.0.jar")
 manifest { attributes("Main-Class" to "net.cnkd.analytics.ReportService") }
 duplicatesStrategy=DuplicatesStrategy.EXCLUDE
 from(configurations.runtimeClasspath.get().map { if(it.isDirectory) it else zipTree(it) })
 exclude("META-INF/*.SF","META-INF/*.RSA","META-INF/*.DSA","module-info.class","META-INF/versions/**")
}
