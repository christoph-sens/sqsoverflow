# sqsoverflow

Kotlin port of [amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib):
transparently offloads SQS message bodies that exceed the 256 KB limit to S3, built on
[aws-sdk-kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [s3overflow](https://github.com/christoph-sens/s3overflow)
(a Kotlin reimplementation of `payload-offloading-java-common-lib-for-aws`, the library the original depends on).

This is a derivative work of the original under the Apache License, Version 2.0 — see
[NOTICE](NOTICE) for exactly which parts were ported and what was changed.

## Why a port

The original ships two ~1000-line classes (`AmazonSQSExtendedClient` for `SqsClient`,
`AmazonSQSExtendedAsyncClient` for `SqsAsyncClient`) that are almost entirely pass-through
boilerplate to the wrapped SQS client, plus a configuration class inherited from
`payload-offloading-java-common-lib-for-aws`.

| Original | Here |
|---|---|
| Separate `AmazonSQSExtendedClient`/`AmazonSQSExtendedAsyncClient`, ~1150-line `AmazonSQSExtendedClientBase` of pure pass-through methods | `aws-sdk-kotlin`'s `SqsClient` is already `suspend`-based, so one `SqsExtendedClient` covers both; Kotlin interface delegation (`SqsClient by sqsClient`) replaces the entire pass-through base class — only the handful of methods with real offload logic are overridden |
| `payloadoffloading-common`'s `PayloadStore`/`S3BackedPayloadStore`/`S3Dao`/`Util`/`PayloadS3Pointer` | [s3overflow](https://github.com/christoph-sens/s3overflow) |
| `ExtendedClientConfiguration` extends `PayloadStorageConfiguration` (S3 client, `ObjectCannedACL`, `ServerSideEncryptionStrategy`, legacy `SQSLargePayloadSize` attribute toggle, deprecated `withLargePayloadSupport*` aliases) | `SqsExtendedClientConfig` takes a `PayloadStore` directly; ACL/CSE-style config is dropped — encryption is configured on the S3 bucket itself, same simplification s3overflow already made |
| 8 main classes, ~3700 lines | 3 files, ~250 lines |

**Note:** dropped compared to the original: client-side `ObjectCannedACL`/`ServerSideEncryptionStrategy`
configuration (bucket-level SSE-S3/SSE-KMS instead), the legacy `SQSLargePayloadSize` attribute name
toggle (always uses `ExtendedPayloadSize`), and the deprecated pre-`PayloadStorageConfiguration` aliases.
Core behavior — threshold-based offloading, `alwaysThroughS3`, `cleanupS3Payload`, `s3KeyPrefix`,
`ignorePayloadNotFound` — is preserved. The receipt-handle and pointer JSON formats are not
byte-identical to the original (see [s3overflow](https://github.com/christoph-sens/s3overflow)'s note
on its pointer format); for a fresh build with no existing Java consumers, that's the simpler choice.

## Usage

```kotlin
val sqsClient = SqsClient.fromEnvironment { region = "eu-central-1" }
val s3Client = S3Client.fromEnvironment { region = "eu-central-1" }

val extendedClient =
    SqsExtendedClient(
        sqsClient,
        SqsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, bucketName = "my-payload-bucket")),
    )

extendedClient.sendMessage(SendMessageRequest { queueUrl = myQueueUrl; messageBody = largePayload })

val messages = extendedClient.receiveMessage(ReceiveMessageRequest { queueUrl = myQueueUrl }).messages
messages?.forEach { message -> extendedClient.deleteMessage(DeleteMessageRequest { queueUrl = myQueueUrl; receiptHandle = message.receiptHandle }) }
```

`SqsExtendedClient` implements `SqsClient`, so it's a drop-in replacement wherever a plain
`aws-sdk-kotlin` `SqsClient` is expected.

## Build

```bash
./gradlew build
```

## Integration tests

`./gradlew integrationTest` runs [SqsExtendedClientIntegrationTest](src/integrationTest/kotlin/com/christophsens/sqsoverflow/SqsExtendedClientIntegrationTest.kt)
against real SQS and S3 APIs via [Testcontainers](https://testcontainers.com)/[Floci](https://github.com/floci-io/floci)
(a free, MIT-licensed local AWS emulator; used instead of LocalStack, whose community edition
now requires an auth token). It
verifies the actual 256 KB offload threshold end-to-end: a message under the threshold is
written straight to SQS with no object created in S3, and a message over the threshold results
in only a pointer on SQS while the payload lands in S3 (and resolves back correctly on
receive). Requires Docker; not part of `./gradlew build`/`check`.

## Installation

Once published to Maven Central:

```kotlin
dependencies {
    implementation("com.christoph-sens:sqsoverflow:0.1.0")
}
```

```xml
<dependency>
  <groupId>com.christoph-sens</groupId>
  <artifactId>sqsoverflow</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Publishing (maintainers)

Publishing uses the [Vanniktech Maven Publish plugin](https://github.com/vanniktech/gradle-maven-publish-plugin)
against Sonatype's Central Publishing Portal. This requires a Central account with the
`com.christoph-sens` namespace verified (via a DNS TXT record on `christoph-sens.com`) and a GPG
signing key. Set the following in `~/.gradle/gradle.properties` (never commit these):

```properties
mavenCentralUsername=...
mavenCentralPassword=...
signing.keyId=...
signing.password=...
signing.secretKeyRingFile=...
```

Then bump `version` in [build.gradle.kts](build.gradle.kts) and run:

```bash
./gradlew publishToMavenCentral
```

## Contributing

Contributions are welcome — see [CONTRIBUTING](CONTRIBUTING.md). This project follows the
[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).

It is a Kotlin port of
[amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib)
(Copyright 2010-2020 Amazon.com, Inc. or its affiliates, also licensed under Apache-2.0) and
therefore a derivative work under that license. See [NOTICE](NOTICE) for exactly which files
and logic were ported, what was changed, and the per-file copyright headers in
`src/main/kotlin/com/christophsens/sqsoverflow`.
