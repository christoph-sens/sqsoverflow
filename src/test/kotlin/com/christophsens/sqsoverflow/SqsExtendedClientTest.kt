package com.christophsens.sqsoverflow

import aws.sdk.kotlin.services.s3.model.NoSuchKey
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.BatchResultErrorEntry
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequest
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchResponse
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityRequest
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityResponse
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchResponse
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchResultEntry
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageResponse
import aws.sdk.kotlin.services.sqs.model.Message
import aws.sdk.kotlin.services.sqs.model.MessageAttributeValue
import aws.sdk.kotlin.services.sqs.model.PurgeQueueRequest
import aws.sdk.kotlin.services.sqs.model.PurgeQueueResponse
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageResponse
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchResponse
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageResponse
import com.christophsens.s3overflow.PayloadS3Pointer
import com.christophsens.s3overflow.PayloadStore
import com.christophsens.s3overflow.SQS_MAX_MESSAGE_SIZE_BYTES
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import kotlin.test.Test

private const val QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/my-queue"

class SqsExtendedClientTest {
    private val sqsClient = mockk<SqsClient>()
    private val payloadStore = mockk<PayloadStore>()
    private val config = SqsExtendedClientConfig(payloadStore, payloadSizeThreshold = 20)
    private val client = SqsExtendedClient(sqsClient, config)

    @Test
    fun `sends a small message unchanged`() =
        runTest {
            coEvery { sqsClient.sendMessage(any<SendMessageRequest>()) } returns SendMessageResponse {}

            client.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "small" })

            coVerify {
                sqsClient.sendMessage(
                    withArg<SendMessageRequest> {
                        assertThat(it.messageBody).isEqualTo("small")
                        assertThat(it.messageAttributes.orEmpty()).isEmpty()
                    },
                )
            }
        }

    @Test
    fun `offloads a message body larger than the threshold`() =
        runTest {
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { sqsClient.sendMessage(any<SendMessageRequest>()) } returns SendMessageResponse {}

            client.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "this message is definitely too large" })

            coVerify {
                sqsClient.sendMessage(
                    withArg<SendMessageRequest> {
                        assertThat(it.messageBody).isEqualTo("pointer-json")
                        assertThat(it.messageAttributes.orEmpty()).containsKey(RESERVED_ATTRIBUTE_NAME)
                    },
                )
            }
        }

    @Test
    fun `offloads every message when alwaysThroughS3 is enabled`() =
        runTest {
            val alwaysS3Client = SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore, alwaysThroughS3 = true))
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { sqsClient.sendMessage(any<SendMessageRequest>()) } returns SendMessageResponse {}

            alwaysS3Client.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "tiny" })

            coVerify {
                sqsClient.sendMessage(withArg<SendMessageRequest> { assertThat(it.messageBody).isEqualTo("pointer-json") })
            }
        }

    @Test
    fun `stores the offloaded payload under the configured s3 key prefix`() =
        runTest {
            val prefixedClient =
                SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore, payloadSizeThreshold = 20, s3KeyPrefix = "orders/"))
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { sqsClient.sendMessage(any<SendMessageRequest>()) } returns SendMessageResponse {}

            prefixedClient.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "this message is definitely too large" })

            coVerify {
                payloadStore.storeOriginalPayload(any(), withArg<String> { assertThat(it).startsWith("orders/") })
            }
        }

    @Test
    fun `rejects a null or empty message body`() {
        assertThatThrownBy {
            runTest { client.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "" }) }
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects a message that already uses the reserved attribute name`() {
        val request =
            SendMessageRequest {
                queueUrl = QUEUE_URL
                messageBody = "small"
                messageAttributes = mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
            }

        assertThatThrownBy { runTest { client.sendMessage(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects a message that uses the legacy reserved attribute name`() {
        val request =
            SendMessageRequest {
                queueUrl = QUEUE_URL
                messageBody = "small"
                messageAttributes = mapOf(LEGACY_RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
            }

        assertThatThrownBy { runTest { client.sendMessage(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects a message with too many attributes`() {
        val attributes =
            (1..MAX_ALLOWED_ATTRIBUTES + 1).associate { i ->
                "attr-$i" to MessageAttributeValue { dataType = "String"; stringValue = "v" }
            }
        val request = SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "small"; messageAttributes = attributes }

        assertThatThrownBy { runTest { client.sendMessage(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `offloads only the batch entries larger than the threshold`() =
        runTest {
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { sqsClient.sendMessageBatch(any<SendMessageBatchRequest>()) } returns
                SendMessageBatchResponse { failed = emptyList(); successful = emptyList() }

            val request =
                SendMessageBatchRequest {
                    queueUrl = QUEUE_URL
                    entries =
                        listOf(
                            SendMessageBatchRequestEntry { id = "small"; messageBody = "tiny" },
                            SendMessageBatchRequestEntry { id = "large"; messageBody = "this message is definitely too large" },
                        )
                }
            client.sendMessageBatch(request)

            coVerify {
                sqsClient.sendMessageBatch(
                    withArg<SendMessageBatchRequest> { batch ->
                        val small = batch.entries.orEmpty().single { it.id == "small" }
                        val large = batch.entries.orEmpty().single { it.id == "large" }
                        assertThat(small.messageBody).isEqualTo("tiny")
                        assertThat(large.messageBody).isEqualTo("pointer-json")
                    },
                )
            }
        }

    @Test
    fun `resolves an offloaded payload on receive and embeds the pointer in the receipt handle`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            coEvery { payloadStore.getOriginalPayload(pointer.toJson()) } returns "original payload"
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse {
                    messages =
                        listOf(
                            Message {
                                body = pointer.toJson()
                                receiptHandle = "original-handle"
                                messageAttributes =
                                    mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "17" })
                            },
                        )
                }

            val response = client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            val message = response.messages.orEmpty().single()
            assertThat(message.body).isEqualTo("original payload")
            assertThat(message.messageAttributes.orEmpty()).doesNotContainKey(RESERVED_ATTRIBUTE_NAME)
            assertThat(isS3ReceiptHandle(message.receiptHandle!!)).isTrue()
            assertThat(originalReceiptHandle(message.receiptHandle!!)).isEqualTo("original-handle")

            coVerify {
                sqsClient.receiveMessage(
                    withArg<ReceiveMessageRequest> {
                        assertThat(it.messageAttributeNames.orEmpty())
                            .containsExactlyInAnyOrder(RESERVED_ATTRIBUTE_NAME, LEGACY_RESERVED_ATTRIBUTE_NAME)
                    },
                )
            }
        }

    @Test
    fun `resolves a payload flagged with the legacy attribute name of the Java library`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            coEvery { payloadStore.getOriginalPayload(pointer.toJson()) } returns "original payload"
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse {
                    messages =
                        listOf(
                            Message {
                                body = pointer.toJson()
                                receiptHandle = "original-handle"
                                messageAttributes =
                                    mapOf(
                                        LEGACY_RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "17" },
                                        "custom" to MessageAttributeValue { dataType = "String"; stringValue = "kept" },
                                    )
                            },
                        )
                }

            val response = client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            val message = response.messages.orEmpty().single()
            assertThat(message.body).isEqualTo("original payload")
            assertThat(message.messageAttributes.orEmpty().keys).containsExactly("custom")
            assertThat(originalReceiptHandle(message.receiptHandle!!)).isEqualTo("original-handle")
        }

    @Test
    fun `does not duplicate reserved attribute names the caller already requested`() =
        runTest {
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns ReceiveMessageResponse {}

            client.receiveMessage(
                ReceiveMessageRequest {
                    queueUrl = QUEUE_URL
                    messageAttributeNames = listOf("custom", LEGACY_RESERVED_ATTRIBUTE_NAME)
                },
            )

            coVerify {
                sqsClient.receiveMessage(
                    withArg<ReceiveMessageRequest> {
                        assertThat(it.messageAttributeNames.orEmpty())
                            .containsExactlyInAnyOrder("custom", RESERVED_ATTRIBUTE_NAME, LEGACY_RESERVED_ATTRIBUTE_NAME)
                    },
                )
            }
        }

    @Test
    fun `passes through a message that was not offloaded`() =
        runTest {
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse { messages = listOf(Message { body = "plain body"; receiptHandle = "handle" }) }

            val response = client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            assertThat(response.messages.orEmpty().single().body).isEqualTo("plain body")
        }

    @Test
    fun `deletes the sqs message and skips it when the payload is missing and ignorePayloadNotFound is enabled`() =
        runTest {
            val lenientClient = SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore, ignorePayloadNotFound = true))
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            coEvery { payloadStore.getOriginalPayload(pointer.toJson()) } throws mockk<NoSuchKey>()
            coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } returns DeleteMessageResponse {}
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse {
                    messages =
                        listOf(
                            Message {
                                body = pointer.toJson()
                                receiptHandle = "original-handle"
                                messageAttributes =
                                    mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
                            },
                        )
                }

            val response = lenientClient.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            assertThat(response.messages.orEmpty()).isEmpty()
            coVerify {
                sqsClient.deleteMessage(
                    withArg<DeleteMessageRequest> {
                        assertThat(it.queueUrl).isEqualTo(QUEUE_URL)
                        assertThat(it.receiptHandle).isEqualTo("original-handle")
                    },
                )
            }
        }

    @Test
    fun `rethrows when the payload is missing and ignorePayloadNotFound is disabled`() {
        val pointer = PayloadS3Pointer("my-bucket", "my-key")
        val notFound = mockk<NoSuchKey>(relaxed = true)
        coEvery { payloadStore.getOriginalPayload(pointer.toJson()) } throws notFound
        coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse {
                messages =
                    listOf(
                        Message {
                            body = pointer.toJson()
                            receiptHandle = "original-handle"
                            messageAttributes =
                                mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
                        },
                    )
            }

        assertThatThrownBy {
            runTest { client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL }) }
        }.isSameAs(notFound)
    }

    @Test
    fun `deletes the offloaded payload and restores the original receipt handle`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            val s3ReceiptHandle = embedPointerInReceiptHandle("original-handle", pointer)
            coEvery { payloadStore.deleteOriginalPayload(pointer.toJson()) } returns Unit
            coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } returns DeleteMessageResponse {}

            client.deleteMessage(DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = s3ReceiptHandle })

            coVerify { payloadStore.deleteOriginalPayload(pointer.toJson()) }
            coVerify {
                sqsClient.deleteMessage(withArg<DeleteMessageRequest> { assertThat(it.receiptHandle).isEqualTo("original-handle") })
            }
        }

    @Test
    fun `does not delete the offloaded payload when cleanupS3Payload is disabled`() =
        runTest {
            val noCleanupClient = SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore, cleanupS3Payload = false))
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            val s3ReceiptHandle = embedPointerInReceiptHandle("original-handle", pointer)
            coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } returns DeleteMessageResponse {}

            noCleanupClient.deleteMessage(DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = s3ReceiptHandle })

            coVerify(exactly = 0) { payloadStore.deleteOriginalPayload(any()) }
            coVerify {
                sqsClient.deleteMessage(withArg<DeleteMessageRequest> { assertThat(it.receiptHandle).isEqualTo("original-handle") })
            }
        }

    @Test
    fun `passes through a delete for a message that was never offloaded`() =
        runTest {
            coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } returns DeleteMessageResponse {}

            client.deleteMessage(DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = "plain-handle" })

            coVerify(exactly = 0) { payloadStore.deleteOriginalPayload(any()) }
            coVerify {
                sqsClient.deleteMessage(withArg<DeleteMessageRequest> { assertThat(it.receiptHandle).isEqualTo("plain-handle") })
            }
        }

    @Test
    fun `deletes offloaded payloads for a batch and restores original receipt handles`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            val s3ReceiptHandle = embedPointerInReceiptHandle("original-handle", pointer)
            coEvery { payloadStore.deleteOriginalPayload(pointer.toJson()) } returns Unit
            coEvery { sqsClient.deleteMessageBatch(any<DeleteMessageBatchRequest>()) } returns
                DeleteMessageBatchResponse {
                    failed = emptyList()
                    successful = listOf(DeleteMessageBatchResultEntry { id = "offloaded" }, DeleteMessageBatchResultEntry { id = "plain" })
                }

            val request =
                DeleteMessageBatchRequest {
                    queueUrl = QUEUE_URL
                    entries =
                        listOf(
                            DeleteMessageBatchRequestEntry { id = "offloaded"; receiptHandle = s3ReceiptHandle },
                            DeleteMessageBatchRequestEntry { id = "plain"; receiptHandle = "plain-handle" },
                        )
                }
            client.deleteMessageBatch(request)

            coVerify { payloadStore.deleteOriginalPayload(pointer.toJson()) }
            coVerify {
                sqsClient.deleteMessageBatch(
                    withArg<DeleteMessageBatchRequest> { batch ->
                        val offloaded = batch.entries.orEmpty().single { it.id == "offloaded" }
                        val plain = batch.entries.orEmpty().single { it.id == "plain" }
                        assertThat(offloaded.receiptHandle).isEqualTo("original-handle")
                        assertThat(plain.receiptHandle).isEqualTo("plain-handle")
                    },
                )
            }
        }

    @Test
    fun `restores the original receipt handle on changeMessageVisibility`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            val s3ReceiptHandle = embedPointerInReceiptHandle("original-handle", pointer)
            coEvery { sqsClient.changeMessageVisibility(any<ChangeMessageVisibilityRequest>()) } returns ChangeMessageVisibilityResponse {}

            client.changeMessageVisibility(
                ChangeMessageVisibilityRequest { queueUrl = QUEUE_URL; receiptHandle = s3ReceiptHandle; visibilityTimeout = 30 },
            )

            coVerify {
                sqsClient.changeMessageVisibility(
                    withArg<ChangeMessageVisibilityRequest> { assertThat(it.receiptHandle).isEqualTo("original-handle") },
                )
            }
        }

    @Test
    fun `restores original receipt handles on changeMessageVisibilityBatch`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            val s3ReceiptHandle = embedPointerInReceiptHandle("original-handle", pointer)
            coEvery { sqsClient.changeMessageVisibilityBatch(any<ChangeMessageVisibilityBatchRequest>()) } returns
                ChangeMessageVisibilityBatchResponse { failed = emptyList(); successful = emptyList() }

            val request =
                ChangeMessageVisibilityBatchRequest {
                    queueUrl = QUEUE_URL
                    entries = listOf(ChangeMessageVisibilityBatchRequestEntry { id = "1"; receiptHandle = s3ReceiptHandle })
                }
            client.changeMessageVisibilityBatch(request)

            coVerify {
                sqsClient.changeMessageVisibilityBatch(
                    withArg<ChangeMessageVisibilityBatchRequest> { batch ->
                        assertThat(batch.entries.orEmpty().single().receiptHandle).isEqualTo("original-handle")
                    },
                )
            }
        }

    @Test
    fun `delegates purgeQueue`() =
        runTest {
            coEvery { sqsClient.purgeQueue(any<PurgeQueueRequest>()) } returns PurgeQueueResponse {}

            client.purgeQueue(PurgeQueueRequest { queueUrl = QUEUE_URL })

            coVerify { sqsClient.purgeQueue(any<PurgeQueueRequest>()) }
        }

    @Test
    fun `sends a small message with ten attributes without offloading it`() =
        runTest {
            val largeThresholdClient = SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore))
            coEvery { sqsClient.sendMessage(any<SendMessageRequest>()) } returns SendMessageResponse {}
            val attributes = (1..10).associate { i -> "attr-$i" to MessageAttributeValue { dataType = "String"; stringValue = "v" } }

            largeThresholdClient.sendMessage(SendMessageRequest { queueUrl = QUEUE_URL; messageBody = "small"; messageAttributes = attributes })

            coVerify { sqsClient.sendMessage(withArg<SendMessageRequest> { assertThat(it.messageAttributes).hasSize(10) }) }
        }

    @Test
    fun `offloads the largest entries until the whole batch fits into the SQS limit`() =
        runTest {
            val largeThresholdClient = SqsExtendedClient(sqsClient, SqsExtendedClientConfig(payloadStore))
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { sqsClient.sendMessageBatch(any<SendMessageBatchRequest>()) } returns
                SendMessageBatchResponse { failed = emptyList(); successful = emptyList() }
            val entrySizes = mapOf("small" to 300_000, "large" to 500_000, "medium" to 400_000)
            check(entrySizes.values.sum() > SQS_MAX_MESSAGE_SIZE_BYTES && entrySizes.values.all { it < SQS_MAX_MESSAGE_SIZE_BYTES })

            largeThresholdClient.sendMessageBatch(
                SendMessageBatchRequest {
                    queueUrl = QUEUE_URL
                    entries = entrySizes.map { (name, size) -> SendMessageBatchRequestEntry { id = name; messageBody = "x".repeat(size) } }
                },
            )

            coVerify {
                sqsClient.sendMessageBatch(
                    withArg<SendMessageBatchRequest> { batch ->
                        val offloaded = batch.entries.orEmpty().filter { it.messageBody == "pointer-json" }.map { it.id }
                        assertThat(offloaded).containsExactly("large")
                    },
                )
            }
        }

    @Test
    fun `resolves the payloads of a batch concurrently`() =
        runTest {
            val pointers = listOf(PayloadS3Pointer("my-bucket", "key-1"), PayloadS3Pointer("my-bucket", "key-2"))
            pointers.forEach { pointer ->
                coEvery { payloadStore.getOriginalPayload(pointer.toJson()) } coAnswers {
                    delay(1_000)
                    "payload of ${pointer.s3Key}"
                }
            }
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse { messages = pointers.map { offloadedMessage(it, receipt = it.s3Key) } }

            val response = client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            assertThat(response.messages.orEmpty().map { it.body }).containsExactly("payload of key-1", "payload of key-2")
            assertThat(currentTime).isEqualTo(1_000)
        }

    @Test
    fun `leaves a message with an unresolvable payload in the queue and returns the others`() =
        runTest {
            val good = PayloadS3Pointer("my-bucket", "good")
            val broken = PayloadS3Pointer("my-bucket", "broken")
            coEvery { payloadStore.getOriginalPayload(good.toJson()) } returns "good payload"
            coEvery { payloadStore.getOriginalPayload(broken.toJson()) } throws IllegalStateException("S3 unavailable")
            coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
                ReceiveMessageResponse {
                    messages =
                        listOf(
                            offloadedMessage(broken, receipt = "broken-handle"),
                            offloadedMessage(good, receipt = "good-handle"),
                            Message { body = "plain body"; receiptHandle = "plain-handle" },
                        )
                }

            val response = client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL })

            assertThat(response.messages.orEmpty().map { it.body }).containsExactly("good payload", "plain body")
            coVerify(exactly = 0) { sqsClient.deleteMessage(any<DeleteMessageRequest>()) }
        }

    @Test
    fun `throws when no message of a receive could be resolved`() {
        val first = PayloadS3Pointer("my-bucket", "first")
        val second = PayloadS3Pointer("my-bucket", "second")
        val firstFailure = IllegalStateException("first")
        val secondFailure = IllegalStateException("second")
        coEvery { payloadStore.getOriginalPayload(first.toJson()) } throws firstFailure
        coEvery { payloadStore.getOriginalPayload(second.toJson()) } throws secondFailure
        coEvery { sqsClient.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse { messages = listOf(offloadedMessage(first, "h1"), offloadedMessage(second, "h2")) }

        assertThatThrownBy { runTest { client.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL }) } }
            .isSameAs(firstFailure)
            .satisfies({ assertThat(it.suppressed).containsExactly(secondFailure) })
    }

    @Test
    fun `deletes the sqs message before its payload`() =
        runTest {
            val pointer = PayloadS3Pointer("my-bucket", "my-key")
            coEvery { payloadStore.deleteOriginalPayload(pointer.toJson()) } returns Unit
            coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } returns DeleteMessageResponse {}

            client.deleteMessage(
                DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = embedPointerInReceiptHandle("original-handle", pointer) },
            )

            coVerifyOrder {
                sqsClient.deleteMessage(any<DeleteMessageRequest>())
                payloadStore.deleteOriginalPayload(pointer.toJson())
            }
        }

    @Test
    fun `keeps the payload when deleting the sqs message fails`() {
        val pointer = PayloadS3Pointer("my-bucket", "my-key")
        val failure = IllegalStateException("receipt handle expired")
        coEvery { sqsClient.deleteMessage(any<DeleteMessageRequest>()) } throws failure

        assertThatThrownBy {
            runTest {
                client.deleteMessage(
                    DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = embedPointerInReceiptHandle("original-handle", pointer) },
                )
            }
        }.isSameAs(failure)
        coVerify(exactly = 0) { payloadStore.deleteOriginalPayload(any()) }
    }

    @Test
    fun `keeps the payloads of batch entries that sqs failed to delete`() =
        runTest {
            val deleted = PayloadS3Pointer("my-bucket", "deleted")
            val kept = PayloadS3Pointer("my-bucket", "kept")
            coEvery { payloadStore.deleteOriginalPayload(deleted.toJson()) } returns Unit
            coEvery { sqsClient.deleteMessageBatch(any<DeleteMessageBatchRequest>()) } returns
                DeleteMessageBatchResponse {
                    successful = listOf(DeleteMessageBatchResultEntry { id = "deleted" })
                    failed = listOf(BatchResultErrorEntry { id = "kept"; code = "ReceiptHandleIsInvalid"; senderFault = true })
                }

            client.deleteMessageBatch(
                DeleteMessageBatchRequest {
                    queueUrl = QUEUE_URL
                    entries =
                        listOf(
                            DeleteMessageBatchRequestEntry { id = "deleted"; receiptHandle = embedPointerInReceiptHandle("h1", deleted) },
                            DeleteMessageBatchRequestEntry { id = "kept"; receiptHandle = embedPointerInReceiptHandle("h2", kept) },
                        )
                },
            )

            coVerify(exactly = 1) { payloadStore.deleteOriginalPayload(deleted.toJson()) }
            coVerify(exactly = 0) { payloadStore.deleteOriginalPayload(kept.toJson()) }
        }

    private fun offloadedMessage(pointer: PayloadS3Pointer, receipt: String): Message =
        Message {
            messageId = pointer.s3Key
            body = pointer.toJson()
            receiptHandle = receipt
            messageAttributes = mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
        }
}
