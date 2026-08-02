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
import com.christophsens.s3overflow.payloadSizeInBytes
import com.christophsens.s3overflow.storeOriginalPayload
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Wraps an aws-sdk-kotlin [SqsClient] and transparently offloads message bodies that exceed
 * [SqsExtendedClientConfig.payloadSizeThreshold] to the configured payload store, sending only a
 * pointer through SQS. On receive, the pointer is resolved back to the original payload and the
 * pointer is embedded in the receipt handle so a later delete can clean up the stored payload too.
 */
class SqsExtendedClient(
    private val sqsClient: SqsClient,
    private val clientConfig: SqsExtendedClientConfig,
) : SqsClient by sqsClient {
    override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
        val body = requireNotNull(input.messageBody?.takeIf { it.isNotEmpty() }) {
            "messageBody cannot be null or empty."
        }
        checkMessageAttributes(input.messageAttributes)

        val request =
            if (clientConfig.alwaysThroughS3 || isLarge(body, input.messageAttributes)) {
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
        val entries = input.entries.orEmpty().map { entry -> offloadIfNeeded(entry) }
        return sqsClient.sendMessageBatch(input.copy { this.entries = entries })
    }

    override suspend fun receiveMessage(input: ReceiveMessageRequest): ReceiveMessageResponse {
        val request =
            input.copy {
                messageAttributeNames =
                    input.messageAttributeNames.orEmpty() - RESERVED_ATTRIBUTE_NAME + RESERVED_ATTRIBUTE_NAME
            }
        val response = sqsClient.receiveMessage(request)
        val messages = response.messages.orEmpty().mapNotNull { message -> resolvePayload(request.queueUrl, message) }
        return response.copy { this.messages = messages }
    }

    override suspend fun deleteMessage(input: DeleteMessageRequest): DeleteMessageResponse {
        val handle = requireNotNull(input.receiptHandle) { "receiptHandle cannot be null." }
        return sqsClient.deleteMessage(input.copy { receiptHandle = originalReceiptHandleAndCleanUp(handle) })
    }

    override suspend fun deleteMessageBatch(input: DeleteMessageBatchRequest): DeleteMessageBatchResponse {
        val entries = input.entries.orEmpty().map { entry -> restoreReceiptHandle(entry) }
        return sqsClient.deleteMessageBatch(input.copy { this.entries = entries })
    }

    override suspend fun changeMessageVisibility(input: ChangeMessageVisibilityRequest): ChangeMessageVisibilityResponse {
        val handle = input.receiptHandle
        val request =
            if (handle != null && isS3ReceiptHandle(handle)) {
                input.copy { receiptHandle = originalReceiptHandle(handle) }
            } else {
                input
            }
        return sqsClient.changeMessageVisibility(request)
    }

    override suspend fun changeMessageVisibilityBatch(
        input: ChangeMessageVisibilityBatchRequest,
    ): ChangeMessageVisibilityBatchResponse {
        val entries =
            input.entries.orEmpty().map { entry ->
                if (isS3ReceiptHandle(entry.receiptHandle)) {
                    entry.copy { receiptHandle = originalReceiptHandle(entry.receiptHandle) }
                } else {
                    entry
                }
            }
        return sqsClient.changeMessageVisibilityBatch(input.copy { this.entries = entries })
    }

    override suspend fun purgeQueue(input: PurgeQueueRequest): PurgeQueueResponse {
        logger.warn { "purgeQueue deletes SQS messages without deleting their offloaded payloads." }
        return sqsClient.purgeQueue(input)
    }

    private suspend fun offloadIfNeeded(entry: SendMessageBatchRequestEntry): SendMessageBatchRequestEntry {
        checkMessageAttributes(entry.messageAttributes)

        val body = entry.messageBody
        if (!clientConfig.alwaysThroughS3 && !isLarge(body, entry.messageAttributes)) return entry

        val pointer = storeOriginalPayload(body)
        return entry.copy {
            messageAttributes = withSizeAttribute(entry.messageAttributes, payloadSizeInBytes(body))
            messageBody = pointer
        }
    }

    private suspend fun resolvePayload(queueUrl: String?, message: Message): Message? {
        if (RESERVED_ATTRIBUTE_NAME !in message.messageAttributes.orEmpty()) return message
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
            messageAttributes = message.messageAttributes.orEmpty() - RESERVED_ATTRIBUTE_NAME
            this.receiptHandle = embedPointerInReceiptHandle(receiptHandle, PayloadS3Pointer.fromJson(pointerJson))
        }
    }

    private suspend fun originalReceiptHandleAndCleanUp(receiptHandle: String): String {
        if (!isS3ReceiptHandle(receiptHandle)) return receiptHandle
        if (clientConfig.cleanupS3Payload) {
            clientConfig.payloadStore.deleteOriginalPayload(pointerFromReceiptHandle(receiptHandle).toJson())
        }
        return originalReceiptHandle(receiptHandle)
    }

    private suspend fun restoreReceiptHandle(entry: DeleteMessageBatchRequestEntry): DeleteMessageBatchRequestEntry {
        return entry.copy { receiptHandle = originalReceiptHandleAndCleanUp(entry.receiptHandle) }
    }

    private suspend fun storeOriginalPayload(body: String): String {
        val prefix = clientConfig.s3KeyPrefix
        return if (prefix.isEmpty()) {
            clientConfig.payloadStore.storeOriginalPayload(body)
        } else {
            clientConfig.payloadStore.storeOriginalPayload(body, prefix + UUID.randomUUID())
        }
    }

    private fun checkMessageAttributes(attributes: Map<String, MessageAttributeValue>?) {
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
        require(RESERVED_ATTRIBUTE_NAME !in attrs) {
            "Message attribute name $RESERVED_ATTRIBUTE_NAME is reserved for use by SqsExtendedClient."
        }
    }

    private fun isLarge(body: String, attributes: Map<String, MessageAttributeValue>?): Boolean =
        attributesSizeInBytes(attributes.orEmpty()) + payloadSizeInBytes(body) > clientConfig.payloadSizeThreshold

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
