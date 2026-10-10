# sqsoverflow

[![Maven Central](https://img.shields.io/maven-central/v/com.christoph-sens/sqsoverflow)](https://central.sonatype.com/artifact/com.christoph-sens/sqsoverflow)
[![CI](https://github.com/christoph-sens/sqsoverflow/actions/workflows/ci.yml/badge.svg)](https://github.com/christoph-sens/sqsoverflow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**An SQS extended client for Kotlin.** `SqsExtendedClient(...)` returns an [aws-sdk-kotlin](https://github.com/awslabs/aws-sdk-kotlin)
`SqsClient` that transparently offloads message bodies above a configurable size threshold to S3 and
resolves them again on receive: the claim-check pattern, with coroutines and without the Java SDK.

AWS provides this pattern for Java as [amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib),
which wraps the AWS SDK for Java v2 clients and can't be used with aws-sdk-kotlin's clients. sqsoverflow
brings the pattern to aws-sdk-kotlin. It is derived from the Java library under the Apache License,
Version 2.0 (see [NOTICE](NOTICE) for exactly which parts were taken over and what was changed) and
built on [s3overflow](https://github.com/christoph-sens/s3overflow), but designed for Kotlin rather than
translated line by line. It is **not wire-compatible** with the Java library.

Part of a family: [s3overflow](https://github.com/christoph-sens/s3overflow) (payload store) · **sqsoverflow** (SQS client) · [snsoverflow](https://github.com/christoph-sens/snsoverflow) (SNS client).

Background and design notes: [Large SQS and SNS messages in Kotlin](https://christoph-sens.github.io/2026/09/large-sqs-sns-messages-in-kotlin/) on the [blog](https://christoph-sens.github.io/).

> **Message size limit:** SQS accepts messages up to 1 MiB, which is the default `payloadSizeThreshold`
> (`SQS_MAX_MESSAGE_SIZE_BYTES`). Before version 1.1.0 the default was 256 KiB; pass
> `payloadSizeThreshold = 256 * 1024` to keep offloading at that size.
> `sendMessageBatch` offloads every entry above the threshold and, if the batch as a whole would still
> exceed the 1 MiB SQS limit for the sum of all messages, the largest remaining entries until it fits.

## Designed for Kotlin

The Java library ships two ~1000-line classes (`AmazonSQSExtendedClient` for `SqsClient`,
`AmazonSQSExtendedAsyncClient` for `SqsAsyncClient`) that are almost entirely pass-through
boilerplate to the wrapped SQS client, plus a configuration class inherited from
`payload-offloading-java-common-lib-for-aws`. With aws-sdk-kotlin, most of that isn't needed:

| Java library | sqsoverflow |
|---|---|
| Separate `AmazonSQSExtendedClient`/`AmazonSQSExtendedAsyncClient`, ~1150-line `AmazonSQSExtendedClientBase` of pure pass-through methods | `aws-sdk-kotlin`'s `SqsClient` is already `suspend`-based, so one `SqsExtendedClient` covers both; a dynamic proxy forwards every other operation to the wrapped client, so only the eight methods with real offload logic are implemented — and operations added by newer aws-sdk-kotlin versions keep working without a new release |
| `payloadoffloading-common`'s `PayloadStore`/`S3BackedPayloadStore`/`S3Dao`/`Util`/`PayloadS3Pointer` | [s3overflow](https://github.com/christoph-sens/s3overflow) |
| `ExtendedClientConfiguration` extends `PayloadStorageConfiguration` (S3 client, `ObjectCannedACL`, `ServerSideEncryptionStrategy`, legacy `SQSLargePayloadSize` attribute toggle, deprecated `withLargePayloadSupport*` aliases) | `SqsExtendedClientConfig` takes a `PayloadStore` directly; ACL/CSE-style config is dropped — encryption is configured on the S3 bucket itself, same simplification s3overflow already made |
| 8 main classes, ~3700 lines | 3 files, ~250 lines |

**Note:** dropped compared to the Java library: client-side `ObjectCannedACL`/`ServerSideEncryptionStrategy`
configuration (bucket-level SSE-S3/SSE-KMS instead), the legacy `SQSLargePayloadSize` attribute name
toggle (always writes `ExtendedPayloadSize`, but recognizes both names on receive), and the deprecated pre-`PayloadStorageConfiguration` aliases.
Core behavior — threshold-based offloading, `alwaysThroughS3`, `cleanupS3Payload`, `s3KeyPrefix`,
`ignorePayloadNotFound` — is preserved. The receipt-handle and pointer JSON formats are not
byte-identical to the Java library's (see [s3overflow](https://github.com/christoph-sens/s3overflow)'s note
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

`SqsExtendedClient(...)` returns an `SqsClient`, so it's a drop-in replacement wherever a plain
`aws-sdk-kotlin` `SqsClient` is expected. The returned client is a dynamic proxy for the `SqsClient`
interface on your classpath: payload operations go through the offloading logic, every other
operation goes straight to the wrapped client. Upgrading aws-sdk-kotlin independently of sqsoverflow
is safe, including versions that add new SQS operations.

## Behavior worth knowing

- **Concurrency:** payloads of a batch are uploaded, downloaded and deleted concurrently.
- **Unresolvable payloads:** if the payload of a received message can't be read (S3 error, rejected
  pointer), that message is left out of the result and logged; it stays in the queue, becomes visible
  again after the visibility timeout and moves to the dead-letter queue once the redrive policy's
  `maxReceiveCount` is reached. Configure a dead-letter queue. Only if no message of a receive can
  be returned is the first error thrown.
- **Delete order:** the SQS message is deleted first, then its payload. If the SQS delete fails, the
  payload stays so a redelivered message can still be resolved; if the payload delete fails, an
  orphaned object remains for the bucket's lifecycle rule (see
  [s3overflow](https://github.com/christoph-sens/s3overflow#operating-the-payload-bucket)).
- **`ignorePayloadNotFound`** only recognizes missing objects if the consumer may `s3:ListBucket` the
  payload bucket. Without it, S3 answers `AccessDenied` instead of `NoSuchKey`, and the message is
  treated as unresolvable.
- **SNS fan-out:** every subscribed queue receives the same pointer. Set `cleanupS3Payload = false`
  in all subscribers and expire payloads with a lifecycle rule, or the first subscriber to delete
  its message removes the payload for the others.

## Differences from amazon-sqs-java-extended-client-lib

sqsoverflow is **not wire-compatible** with the Java library: the S3 pointer and receipt-handle
formats differ. A message sent by one cannot be read by the other, so all producers and consumers
of a queue must use the same library. A Kotlin service that uses the Java library through the Java
SDK today can only switch together with the rest of the queue's producers and consumers, or after
draining the queue.

The reserved message attribute is `ExtendedPayloadSize`, the same name the Java libraries use, so
SNS-to-SQS fan-out works between [snsoverflow](https://github.com/christoph-sens/snsoverflow) and sqsoverflow.

| Java option | In sqsoverflow |
|---|---|
| Client-side encryption (`ServerSideEncryptionStrategy`) | Configure SSE-S3 or SSE-KMS on the bucket |
| `ObjectCannedACL` | Use bucket policies |
| Legacy `SQSLargePayloadSize` attribute name (the Java default) | Recognized on receive; always writes `ExtendedPayloadSize`, which the Java library also reads |

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

It is derived from
[amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib)
(Copyright 2010-2020 Amazon.com, Inc. or its affiliates, also licensed under Apache-2.0) and
therefore a derivative work under that license. See [NOTICE](NOTICE) for exactly which files
and logic were ported, what was changed, and the per-file copyright headers in
`src/main/kotlin/com/christophsens/sqsoverflow`.
