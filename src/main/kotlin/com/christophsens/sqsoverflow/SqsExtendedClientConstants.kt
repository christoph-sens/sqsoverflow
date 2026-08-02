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
 * This file is a Kotlin port of the reserved-attribute, receipt-handle-marker, and
 * S3-key-prefix validation logic from amazon-sqs-java-extended-client-lib's
 * SQSExtendedClientConstants/AmazonSQSExtendedClientUtil
 * (https://github.com/awslabs/amazon-sqs-java-extended-client-lib). RESERVED_ATTRIBUTE_NAME
 * is kept byte-identical to the original's reserved attribute name, since the original
 * documents it as shared with SNSExtendedClient implementations and changing it would break
 * interop with messages produced by the original library. See NOTICE for details.
 */

package com.christophsens.sqsoverflow

import com.christophsens.s3overflow.PayloadS3Pointer

/** Message attribute name used to flag a message whose body was offloaded to S3. */
internal const val RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize"

/** SQS allows at most 10 message attributes; one slot is reserved for [RESERVED_ATTRIBUTE_NAME]. */
internal const val MAX_ALLOWED_ATTRIBUTES = 9

private const val S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
private const val S3_KEY_MARKER = "-..s3Key..-"

private const val MAX_S3_KEY_LENGTH = 1024
private const val UUID_LENGTH = 36
internal const val MAX_S3_KEY_PREFIX_LENGTH = MAX_S3_KEY_LENGTH - UUID_LENGTH

private val INVALID_S3_KEY_PREFIX_CHARACTERS = Regex("[^a-zA-Z0-9./_-]")

/** Validates and trims an S3 key prefix, throwing [IllegalArgumentException] if it isn't usable. */
internal fun validateS3KeyPrefix(s3KeyPrefix: String): String {
    val trimmed = s3KeyPrefix.trim()
    require(trimmed.length <= MAX_S3_KEY_PREFIX_LENGTH) {
        "The S3 key prefix length must not be greater than $MAX_S3_KEY_PREFIX_LENGTH"
    }
    require(!trimmed.startsWith(".") && !trimmed.startsWith("/")) {
        "The S3 key prefix must not start with '.' or '/'"
    }
    require(!trimmed.contains("..")) { "The S3 key prefix must not contain the string '..'" }
    require(!INVALID_S3_KEY_PREFIX_CHARACTERS.containsMatchIn(trimmed)) {
        "The S3 key prefix contains invalid characters. Allowed: letters, digits, '/', '_', '-', and '.'"
    }
    return trimmed
}

/** Embeds the S3 pointer for an offloaded payload into [receiptHandle] so it survives a later delete. */
internal fun embedPointerInReceiptHandle(receiptHandle: String, pointer: PayloadS3Pointer): String =
    S3_BUCKET_NAME_MARKER + pointer.s3BucketName + S3_BUCKET_NAME_MARKER +
        S3_KEY_MARKER + pointer.s3Key + S3_KEY_MARKER +
        receiptHandle

internal fun isS3ReceiptHandle(receiptHandle: String): Boolean =
    receiptHandle.contains(S3_BUCKET_NAME_MARKER) && receiptHandle.contains(S3_KEY_MARKER)

/** Strips the embedded S3 pointer back off, returning the receipt handle SQS actually issued. */
internal fun originalReceiptHandle(receiptHandle: String): String {
    val secondKeyMarker = receiptHandle.indexOf(S3_KEY_MARKER, receiptHandle.indexOf(S3_KEY_MARKER) + 1)
    return receiptHandle.substring(secondKeyMarker + S3_KEY_MARKER.length)
}

/** Recovers the [PayloadS3Pointer] previously embedded by [embedPointerInReceiptHandle]. */
internal fun pointerFromReceiptHandle(receiptHandle: String): PayloadS3Pointer {
    val bucketName = between(receiptHandle, S3_BUCKET_NAME_MARKER)
    val key = between(receiptHandle, S3_KEY_MARKER)
    return PayloadS3Pointer(bucketName, key)
}

private fun between(receiptHandle: String, marker: String): String {
    val first = receiptHandle.indexOf(marker)
    val second = receiptHandle.indexOf(marker, first + 1)
    return receiptHandle.substring(first + marker.length, second)
}
