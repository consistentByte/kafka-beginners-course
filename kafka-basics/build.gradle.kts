plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
//    testImplementation(platform("org.junit:junit-bom:5.10.0"))
//    testImplementation("org.junit.jupiter:junit-jupiter")
//    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
// Source: https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:4.1.0")
    // Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:1.7.36")
// Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:1.7.36")
}

tasks.test {
    useJUnitPlatform()
}