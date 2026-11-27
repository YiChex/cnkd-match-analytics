plugins { java }
allprojects { repositories { maven("https://maven.aliyun.com/repository/public"); mavenCentral() } }
subprojects {
 apply(plugin="java")
 group="net.cnkd.analytics"
 version="0.1.0"
 tasks.withType<JavaCompile>().configureEach { options.encoding="UTF-8"; options.release.set(if(project.name=="report-service")17 else 11) }
 dependencies { "testImplementation"("org.junit.jupiter:junit-jupiter:5.10.1") }
 tasks.withType<Test>().configureEach { useJUnitPlatform() }
}
