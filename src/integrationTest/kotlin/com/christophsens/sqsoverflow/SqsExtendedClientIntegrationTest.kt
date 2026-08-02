package com.christophsens.sqsoverflow

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.CreateQueueRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.christophsens.s3overflow.PayloadS3Pointer
import com.christophsens.s3overflow.S3BackedPayloadStore
import com.christophsens.s3overflow.SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES
import io.floci.testcontainers.FlociContainer
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.UUID

/**
 * Runs [SqsExtendedClient] against real SQS and S3 APIs (via [Floci](https://github.com/floci-io/floci),
 * a free local AWS emulator) to verify the actual 256 KB offload threshold end-to-end: small
 * messages go straight to SQS, larger ones are written to S3 and only a pointer travels through
 * SQS. Requires Docker.
 *
 * Run with `./gradlew integrationTest`; this is not part of `./gradlew build`/`check`.
 */
@Testcontainers
class SqsExtendedClientIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val floci: FlociContainer = FlociContainer()
    }

    private val credentialsProvider =
        StaticCredentialsProvider {
            accessKeyId = floci.accessKey
            secretAccessKey = floci.secretKey
        }

    private val sqsClient =
        SqsClient {
            endpointUrl = Url.parse(floci.endpoint)
            region = floci.region
            credentialsProvider = this@SqsExtendedClientIntegrationTest.credentialsProvider
        }

    private val s3Client =
        S3Client {
            endpointUrl = Url.parse(floci.endpoint)
            region = floci.region
            credentialsProvider = this@SqsExtendedClientIntegrationTest.credentialsProvider
            forcePathStyle = true
        }

    private lateinit var testQueueUrl: String
    private lateinit var testBucketName: String
    private lateinit var extendedClient: SqsExtendedClient

    @BeforeEach
    fun setUp() =
        runTest {
            testQueueUrl = sqsClient.createQueue(CreateQueueRequest { queueName = "test-queue-${UUID.randomUUID()}" }).queueUrl!!
            testBucketName = "test-bucket-${UUID.randomUUID()}"
            s3Client.createBucket(CreateBucketRequest { bucket = testBucketName })

            extendedClient =
                SqsExtendedClient(
                    sqsClient,
                    SqsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, testBucketName)),
                )
        }

    @Test
    fun `messages smaller than 256 KB are written directly to SQS, not to S3`() =
        runTest {
            val smallBody = "x".repeat(1024)

            extendedClient.sendMessage(SendMessageRequest { queueUrl = testQueueUrl; messageBody = smallBody })

            val rawMessage = receiveRawMessage()
            assertThat(rawMessage.body).isEqualTo(smallBody)
            assertThat(rawMessage.messageAttributes.orEmpty()).doesNotContainKey(RESERVED_ATTRIBUTE_NAME)
            assertThat(objectsInBucket()).isEmpty()
        }

    @Test
    fun `messages larger than 256 KB are stored in S3 and only a pointer travels through SQS`() =
        runTest {
            val largeBody = "x".repeat(SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES + 1024)

            extendedClient.sendMessage(SendMessageRequest { queueUrl = testQueueUrl; messageBody = largeBody })

            val rawMessage = receiveRawMessage()
            assertThat(rawMessage.body).isNotEqualTo(largeBody)
            assertThat(rawMessage.messageAttributes.orEmpty()).containsKey(RESERVED_ATTRIBUTE_NAME)

            val pointer = PayloadS3Pointer.fromJson(requireNotNull(rawMessage.body))
            assertThat(pointer.s3BucketName).isEqualTo(testBucketName)

            val objects = objectsInBucket()
            assertThat(objects).hasSize(1)
            assertThat(objects.single().key).isEqualTo(pointer.s3Key)

            val resolvedMessage =
                extendedClient.receiveMessage(ReceiveMessageRequest { queueUrl = testQueueUrl; waitTimeSeconds = 5 })
                    .messages.orEmpty().single()
            assertThat(resolvedMessage.body).isEqualTo(largeBody)
        }

    /** Receives with visibilityTimeout = 0 so the message stays visible for a later receive in the same test. */
    private suspend fun receiveRawMessage() =
        sqsClient.receiveMessage(
            ReceiveMessageRequest {
                queueUrl = testQueueUrl
                waitTimeSeconds = 5
                visibilityTimeout = 0
                messageAttributeNames = listOf("All")
            },
        ).messages.orEmpty().single()

    private suspend fun objectsInBucket() =
        s3Client.listObjectsV2(ListObjectsV2Request { bucket = testBucketName }).contents.orEmpty()
}
