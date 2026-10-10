/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2026 Christoph Sens
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "LICENSE" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * This file is a Kotlin port of the message-offload control flow from
 * amazon-sqs-java-extended-client-lib's AmazonSQSExtendedClient/-ClientBase/-ClientUtil
 * (https://github.com/awslabs/amazon-sqs-java-extended-client-lib), adapted for
 * aws-sdk-kotlin, coroutines, and s3overflow. See NOTICE for details.
 */

package com.christophsens.sqsoverflow

import aws.sdk.kotlin.services.s3.model.NoSuchKey
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequest
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchResponse
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityRequest
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityResponse
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchResponse
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
import com.christophsens.s3overflow.SQS_MAX_MESSAGE_SIZE_BYTES
import com.christophsens.s3overflow.payloadSizeInBytes
import com.christophsens.s3overflow.storeOriginalPayload
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException

private val logger = KotlinLogging.logger {}

/**
 * Returns an [SqsClient] that wraps [sqsClient] and transparently offloads message bodies that exceed
 * [SqsExtendedClientConfig.payloadSizeThreshold] to the configured payload store, sending only a
 * pointer through SQS. On receive, the pointer is resolved back to the original payload and the
 * pointer is embedded in the receipt handle so a later delete can clean up the stored payload too.
 *
 * Payloads of a batch are stored, resolved and deleted concurrently. A received message whose payload
 * can't be resolved is left in the queue instead of failing the whole receive: it becomes visible again
 * after the visibility timeout and ends up in the dead-letter queue if one is configured.
 *
 * The returned client is a dynamic proxy for the [SqsClient] interface of the aws-sdk-kotlin version on
 * the runtime classpath: the operations that handle payloads go through the offloading logic, every other
 * operation goes straight to [sqsClient]. Operations added by a newer aws-sdk-kotlin therefore work
 * without a new sqsoverflow release.
 */
fun SqsExtendedClient(
    sqsClient: SqsClient,
    clientConfig: SqsExtendedClientConfig,
): SqsClient = offloadingProxy(OffloadingSqsClient(sqsClient, clientConfig), sqsClient, OFFLOADED_OPERATIONS)

/** Operations that [OffloadingSqsClient] overrides; all others are forwarded to the wrapped client by the proxy. */
internal val OFFLOADED_OPERATIONS =
    setOf(
        "sendMessage",
        "sendMessageBatch",
        "receiveMessage",
        "deleteMessage",
        "deleteMessageBatch",
        "changeMessageVisibility",
        "changeMessageVisibilityBatch",
        "purgeQueue",
    )

/**
 * The offloading logic behind [SqsExtendedClient]. Only reached through the proxy, and only for
 * [OFFLOADED_OPERATIONS]: its delegated members are compiled against one aws-sdk-kotlin version.
 */
internal class OffloadingSqsClient(
    private val sqsClient: SqsClient,
    private val clientConfig: SqsExtendedClientConfig,
) : SqsClient by sqsClient {
    override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
        val body = requireNotNull(input.messageBody?.takeIf { it.isNotEmpty() }) {
            "messageBody cannot be null or empty."
        }
        checkReservedAttributes(input.messageAttributes)

        val request =
            if (clientConfig.alwaysThroughS3 || isLarge(body, input.messageAttributes)) {
                checkOffloadableAttributes(input.messageAttributes)
                val pointer = storeOriginalPayload(body)
                input.copy {
                    messageAttributes = withSizeAttribute(input.messageAttributes, payloadSizeInBytes(body))
                    messageBody = pointer
                }
            } else {
                input
            }
        return sqsClient.sendMessage(request)
    }

    override suspend fun sendMessageBatch(input: SendMessageBatchRequest): SendMessageBatchResponse {
        val entries = input.entries.orEmpty()
        entries.forEach { entry -> checkReservedAttributes(entry.messageAttributes) }

        val toOffload = entriesToOffload(entries)
        val prepared =
            coroutineScope {
                entries.mapIndexed { index, entry -> async { if (index in toOffload) offload(entry) else entry } }.awaitAll()
            }
        return sqsClient.sendMessageBatch(input.copy { this.entries = prepared })
    }

    override suspend fun receiveMessage(input: ReceiveMessageRequest): ReceiveMessageResponse {
        val request =
            input.copy {
                messageAttributeNames =
                    input.messageAttributeNames.orEmpty() - RESERVED_ATTRIBUTE_NAMES.toSet() + RESERVED_ATTRIBUTE_NAMES
            }
        val response = sqsClient.receiveMessage(request)
        val results =
            coroutineScope {
                response.messages.orEmpty().map { message -> async { resolveOrFailure(request.queueUrl, message) } }.awaitAll()
            }

        val failures = results.filterIsInstance<Resolution.Failed>()
        val messages = results.filterIsInstance<Resolution.Resolved>().mapNotNull { it.message }
        if (failures.isNotEmpty() && messages.isEmpty()) {
            throw failures.first().cause.apply { failures.drop(1).forEach { addSuppressed(it.cause) } }
        }
        failures.forEach { failure ->
            logger.error(failure.cause) {
                "Payload of message ${failure.messageId} could not be resolved; it stays in the queue and is redelivered " +
                    "after the visibility timeout."
            }
        }
        return response.copy { this.messages = messages }
    }

    override suspend fun deleteMessage(input: DeleteMessageRequest): DeleteMessageResponse {
        val handle = requireNotNull(input.receiptHandle) { "receiptHandle cannot be null." }
        val response = sqsClient.deleteMessage(input.copy { receiptHandle = originalReceiptHandleOrSelf(handle) })
        cleanUpPayload(handle)
        return response
    }

    override suspend fun deleteMessageBatch(input: DeleteMessageBatchRequest): DeleteMessageBatchResponse {
        val entries = input.entries.orEmpty()
        val response =
            sqsClient.deleteMessageBatch(
                input.copy {
                    this.entries = entries.map { entry -> entry.copy { receiptHandle = originalReceiptHandleOrSelf(entry.receiptHandle) } }
                },
            )

        val deletedIds = response.successful.map { it.id }.toSet()
        coroutineScope {
            entries.filter { it.id in deletedIds }.map { entry -> async { cleanUpPayload(entry.receiptHandle) } }.awaitAll()
        }
        return response
    }

    override suspend fun changeMessageVisibility(input: ChangeMessageVisibilityRequest): ChangeMessageVisibilityResponse {
        val handle = input.receiptHandle
        val request = if (handle != null) input.copy { receiptHandle = originalReceiptHandleOrSelf(handle) } else input
        return sqsClient.changeMessageVisibility(request)
    }

    override suspend fun changeMessageVisibilityBatch(
        input: ChangeMessageVisibilityBatchRequest,
    ): ChangeMessageVisibilityBatchResponse {
        val entries = input.entries.orEmpty().map { entry -> entry.copy { receiptHandle = originalReceiptHandleOrSelf(entry.receiptHandle) } }
        return sqsClient.changeMessageVisibilityBatch(input.copy { this.entries = entries })
    }

    override suspend fun purgeQueue(input: PurgeQueueRequest): PurgeQueueResponse {
        logger.warn { "purgeQueue deletes SQS messages without deleting their offloaded payloads." }
        return sqsClient.purgeQueue(input)
    }

    /**
     * Indices of the batch entries to offload: every entry above the threshold, plus the largest remaining
     * entries while the batch as a whole would exceed the SQS limit for the sum of all messages.
     */
    private fun entriesToOffload(entries: List<SendMessageBatchRequestEntry>): Set<Int> {
        val sizes = entries.map { messageSizeInBytes(it.messageBody, it.messageAttributes) }
        val offloadedSizes = entries.map { attributesSizeInBytes(it.messageAttributes.orEmpty()) + OFFLOADED_BODY_ALLOWANCE_BYTES }
        val toOffload = entries.indices.filterTo(mutableSetOf()) { clientConfig.alwaysThroughS3 || sizes[it] > clientConfig.payloadSizeThreshold }

        var total = entries.indices.sumOf { if (it in toOffload) offloadedSizes[it] else sizes[it] }
        val candidates = (entries.indices - toOffload).filter { sizes[it] > offloadedSizes[it] }.sortedByDescending { sizes[it] }.iterator()
        while (total > SQS_MAX_MESSAGE_SIZE_BYTES && candidates.hasNext()) {
            val index = candidates.next()
            toOffload += index
            total += offloadedSizes[index] - sizes[index]
        }
        toOffload.forEach { checkOffloadableAttributes(entries[it].messageAttributes) }
        return toOffload
    }

    private suspend fun offload(entry: SendMessageBatchRequestEntry): SendMessageBatchRequestEntry {
        val body = entry.messageBody
        val pointer = storeOriginalPayload(body)
        return entry.copy {
            messageAttributes = withSizeAttribute(entry.messageAttributes, payloadSizeInBytes(body))
            messageBody = pointer
        }
    }

    private sealed interface Resolution {
        /** [message] is null when the message was deleted because its payload was missing. */
        class Resolved(val message: Message?) : Resolution

        class Failed(val messageId: String?, val cause: Throwable) : Resolution
    }

    private suspend fun resolveOrFailure(queueUrl: String?, message: Message): Resolution =
        try {
            Resolution.Resolved(resolvePayload(queueUrl, message))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Resolution.Failed(message.messageId, e)
        }

    private suspend fun resolvePayload(queueUrl: String?, message: Message): Message? {
        if (RESERVED_ATTRIBUTE_NAMES.none { it in message.messageAttributes.orEmpty() }) return message
        val pointerJson = message.body ?: return message
        val receiptHandle = requireNotNull(message.receiptHandle) { "receiptHandle cannot be null." }

        val originalBody =
            try {
                clientConfig.payloadStore.getOriginalPayload(pointerJson)
            } catch (e: NoSuchKey) {
                if (!clientConfig.ignorePayloadNotFound) throw e
                logger.warn { "Message deleted from SQS since its payload could not be found in the payload store." }
                if (queueUrl != null) {
                    sqsClient.deleteMessage(DeleteMessageRequest { this.queueUrl = queueUrl; this.receiptHandle = receiptHandle })
                }
                return null
            }

        return message.copy {
            this.body = originalBody
            messageAttributes = message.messageAttributes.orEmpty() - RESERVED_ATTRIBUTE_NAMES.toSet()
            this.receiptHandle = embedPointerInReceiptHandle(receiptHandle, PayloadS3Pointer.fromJson(pointerJson))
        }
    }

    private fun originalReceiptHandleOrSelf(receiptHandle: String): String =
        if (isS3ReceiptHandle(receiptHandle)) originalReceiptHandle(receiptHandle) else receiptHandle

    /** Deletes the payload behind [receiptHandle] once its message is gone; a failure only leaves an orphaned object. */
    private suspend fun cleanUpPayload(receiptHandle: String) {
        if (!clientConfig.cleanupS3Payload || !isS3ReceiptHandle(receiptHandle)) return
        clientConfig.payloadStore.deleteOriginalPayload(pointerFromReceiptHandle(receiptHandle).toJson())
    }

    private suspend fun storeOriginalPayload(body: String): String {
        val prefix = clientConfig.s3KeyPrefix
        return if (prefix.isEmpty()) {
            clientConfig.payloadStore.storeOriginalPayload(body)
        } else {
            clientConfig.payloadStore.storeOriginalPayload(body, prefix + UUID.randomUUID())
        }
    }

    private fun checkReservedAttributes(attributes: Map<String, MessageAttributeValue>?) {
        val attrs = attributes.orEmpty()
        RESERVED_ATTRIBUTE_NAMES.forEach { name ->
            require(name !in attrs) { "Message attribute name $name is reserved for use by SqsExtendedClient." }
        }
    }

    private fun checkOffloadableAttributes(attributes: Map<String, MessageAttributeValue>?) {
        val attrs = attributes.orEmpty()
        val size = attributesSizeInBytes(attrs)
        require(size <= clientConfig.payloadSizeThreshold) {
            "Total size of message attributes is $size bytes which is larger than the threshold of " +
                "${clientConfig.payloadSizeThreshold} bytes. Consider including the payload in the message body instead " +
                "of message attributes."
        }
        require(attrs.size <= MAX_ALLOWED_ATTRIBUTES) {
            "Number of message attributes [${attrs.size}] exceeds the maximum allowed for large-payload " +
                "messages [$MAX_ALLOWED_ATTRIBUTES]."
        }
    }

    private fun isLarge(body: String, attributes: Map<String, MessageAttributeValue>?): Boolean =
        messageSizeInBytes(body, attributes) > clientConfig.payloadSizeThreshold

    private fun messageSizeInBytes(body: String, attributes: Map<String, MessageAttributeValue>?): Long =
        attributesSizeInBytes(attributes.orEmpty()) + payloadSizeInBytes(body)

    private fun attributesSizeInBytes(attributes: Map<String, MessageAttributeValue>): Long =
        attributes.entries.sumOf { (key, value) ->
            payloadSizeInBytes(key) +
                payloadSizeInBytes(value.dataType) +
                (value.stringValue?.let(::payloadSizeInBytes) ?: 0) +
                (value.binaryValue?.size?.toLong() ?: 0)
        }

    private fun withSizeAttribute(
        attributes: Map<String, MessageAttributeValue>?,
        payloadSize: Long,
    ): Map<String, MessageAttributeValue> =
        attributes.orEmpty() +
            (
                RESERVED_ATTRIBUTE_NAME to
                    MessageAttributeValue {
                        dataType = "Number"
                        stringValue = payloadSize.toString()
                    }
            )
}
