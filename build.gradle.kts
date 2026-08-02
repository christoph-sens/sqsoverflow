plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.maven.publish)
    `java-library`
}

group = "com.christoph-sens"
version = "0.1.0"

kotlin {
    jvmToolchain(25)
}

sourceSets {
    create("integrationTest") {
        kotlin.srcDir("src/integrationTest/kotlin")
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += output + compileClasspath
    }
}

kotlin.target.compilations.getByName("integrationTest") {
    associateWith(kotlin.target.compilations.getByName("main"))
}

val integrationTestImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    api(libs.aws.sdk.kotlin.sqs)
    api(libs.s3overflow)
    implementation(libs.aws.sdk.kotlin.s3)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlin.logging)
    implementation(libs.slf4j.api)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.mockk)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.slf4j.simple)

    integrationTestImplementation(libs.testcontainers.core)
    integrationTestImplementation(libs.testcontainers.junit.jupiter)
    integrationTestImplementation(libs.testcontainers.floci)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against LocalStack (requires Docker)."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

mavenPublishing {
    pom {
        name.set("sqsoverflow")
        description.set(
            "Kotlin SQS extended client: transparently offloads message payloads that exceed the " +
                "256 KB SQS limit to S3, based on aws-sdk-kotlin, coroutines, and s3overflow.",
        )
        url.set("https://github.com/christoph-sens/sqsoverflow")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("christoph-sens")
                name.set("Christoph Sens")
                url.set("https://github.com/christoph-sens")
            }
        }
        scm {
            url.set("https://github.com/christoph-sens/sqsoverflow")
            connection.set("scm:git:git://github.com/christoph-sens/sqsoverflow.git")
            developerConnection.set("scm:git:ssh://git@github.com/christoph-sens/sqsoverflow.git")
        }
    }

    publishToMavenCentral()
    signAllPublications()
}
