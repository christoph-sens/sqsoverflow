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
 * This file is a Kotlin port of configuration logic from
 * amazon-sqs-java-extended-client-lib's ExtendedClientConfiguration
 * (https://github.com/awslabs/amazon-sqs-java-extended-client-lib), adapted for
 * aws-sdk-kotlin and s3overflow with a reduced configuration surface. See NOTICE for details.
 */

package com.christophsens.sqsoverflow

import com.christophsens.s3overflow.PayloadStore
import com.christophsens.s3overflow.SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES

/**
 * Configures [SqsExtendedClient]. Encryption at rest is configured on the S3 bucket backing
 * [payloadStore] (SSE-S3/SSE-KMS), not here.
 */
class SqsExtendedClientConfig(
    /** Backing store for offloaded payloads, e.g. `S3BackedPayloadStore(s3Client, bucketName)`. */
    val payloadStore: PayloadStore,
    /** Messages whose body + attributes exceed this many bytes are offloaded to [payloadStore]. */
    val payloadSizeThreshold: Int = SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES,
    /** When true, every message is offloaded regardless of [payloadSizeThreshold]. */
    val alwaysThroughS3: Boolean = false,
    /** When true, deleting a message also deletes its offloaded payload. */
    val cleanupS3Payload: Boolean = true,
    /** When true, a message whose payload is missing from the store is deleted instead of failing. */
    val ignorePayloadNotFound: Boolean = false,
    /** Prefix prepended to the generated key of every offloaded payload. */
    s3KeyPrefix: String = "",
) {
    val s3KeyPrefix: String = validateS3KeyPrefix(s3KeyPrefix)
}
