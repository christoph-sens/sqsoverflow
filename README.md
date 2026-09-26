# sqsoverflow

[![Maven Central](https://img.shields.io/maven-central/v/com.christoph-sens/sqsoverflow)](https://central.sonatype.com/artifact/com.christoph-sens/sqsoverflow)
[![CI](https://github.com/christoph-sens/sqsoverflow/actions/workflows/ci.yml/badge.svg)](https://github.com/christoph-sens/sqsoverflow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Kotlin port of [amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib):
transparently offloads SQS message bodies that exceed the configured size threshold to S3, built on
[aws-sdk-kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [s3overflow](https://github.com/christoph-sens/s3overflow)
(a Kotlin reimplementation of `payload-offloading-java-common-lib-for-aws`, the library the original depends on).

This is a derivative work of the original under the Apache License, Version 2.0 — see
[NOTICE](NOTICE) for exactly which parts were ported and what was changed.

Part of a family: [s3overflow](https://github.com/christoph-sens/s3overflow) (payload store) · **sqsoverflow** (SQS client) · [snsoverflow](https://github.com/christoph-sens/snsoverflow) (SNS client).

Background, migration guide and design notes: [Large SQS and SNS messages in Kotlin](https://christoph-sens.github.io/2026/09/large-sqs-sns-messages-in-kotlin/) on the [blog](https://christoph-sens.github.io/).

> **Message size limit:** SQS accepts messages up to 1 MiB, which is the default `payloadSizeThreshold`
> (`SQS_MAX_MESSAGE_SIZE_BYTES`). Before version 1.1.0 the default was 256 KiB; pass
> `payloadSizeThreshold = 256 * 1024` to keep offloading at that size.
> `sendMessageBatch` offloads per entry; the whole batch must still fit into the 1 MiB SQS request limit.

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

## Migrating from amazon-sqs-java-extended-client-lib

sqsoverflow is **not wire-compatible** with the Java library: the S3 pointer and receipt-handle
formats differ. A message sent by one cannot be read by the other. Switch all producers and
consumers of a queue at the same time, or drain the queue before switching.

```java
// Before (Java, amazon-sqs-java-extended-client-lib)
ExtendedClientConfiguration config = new ExtendedClientConfiguration()
    .withPayloadSupportEnabled(s3Client, "my-payload-bucket");
SqsClient client = new AmazonSQSExtendedClient(SqsClient.builder().build(), config);
```

```kotlin
// After (Kotlin, sqsoverflow)
val client = SqsExtendedClient(
    SqsClient.fromEnvironment(),
    SqsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, bucketName = "my-payload-bucket")),
)
```

Client-side encryption and canned ACL options have no equivalent; configure SSE-S3/SSE-KMS on the bucket instead.

## Build

```bash
./gradlew build
```

## Integration tests

`./gradlew integrationTest` runs [SqsExtendedClientIntegrationTest](src/integrationTest/kotlin/com/christophsens/sqsoverflow/SqsExtendedClientIntegrationTest.kt)
against real SQS and S3 APIs via [Testcontainers](https://testcontainers.com)/[Floci](https://github.com/floci-io/floci)
(a free, MIT-licensed local AWS emulator; used instead of LocalStack, whose community edition
now requires an auth token). It
verifies the default 1 MiB offload threshold end-to-end: a message under the threshold is
written straight to SQS with no object created in S3, and a message over the threshold results
in only a pointer on SQS while the payload lands in S3 (and resolves back correctly on
receive). Requires Docker; not part of `./gradlew build`/`check`.

## Installation

```kotlin
dependencies {
    implementation("com.christoph-sens:sqsoverflow:<version>")
}
```

```xml
<dependency>
  <groupId>com.christoph-sens</groupId>
  <artifactId>sqsoverflow</artifactId>
  <version><version></version>
</dependency>
```

### Verifying a release

Every file published to Maven Central (jars, POM, Gradle module metadata) has a signed
[build provenance attestation](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations)
proving it was built by this repository's release workflow from the tagged commit. Verify a
downloaded file with the GitHub CLI:

```bash
gh attestation verify sqsoverflow-<version>.jar --repo christoph-sens/sqsoverflow
```

Releases published before provenance attestations were introduced have no attestation.

## Releasing (maintainers)

Releases are published to Maven Central by the [release workflow](.github/workflows/release.yml)
using the [Vanniktech Maven Publish plugin](https://github.com/vanniktech/gradle-maven-publish-plugin).
The version comes from the Git tag; there is no version to bump in the build file.

```bash
git tag v1.2.3
git push origin v1.2.3
```

The workflow builds and tests the tag, then waits for manual approval in the `maven-central`
environment before signing and publishing. The publish job attests the build provenance of the
published files before uploading them, then creates a GitHub release with generated notes and
the published jars attached. Maven Central releases are immutable: fix mistakes with a new patch release.
Running the workflow manually (`workflow_dispatch`) is a dry run that never publishes.

This library depends on [s3overflow](https://github.com/christoph-sens/s3overflow). When releasing
both, release s3overflow first and wait for Dependabot to bump it here before tagging this repo.

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
