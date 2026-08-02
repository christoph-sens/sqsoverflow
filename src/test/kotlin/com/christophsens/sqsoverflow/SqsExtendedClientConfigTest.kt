package com.christophsens.sqsoverflow

import com.christophsens.s3overflow.PayloadStore
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import kotlin.test.Test

class SqsExtendedClientConfigTest {
    private val payloadStore = mockk<PayloadStore>()

    @Test
    fun `defaults are sensible`() {
        val config = SqsExtendedClientConfig(payloadStore)

        assertThat(config.payloadSizeThreshold).isEqualTo(262_144)
        assertThat(config.alwaysThroughS3).isFalse()
        assertThat(config.cleanupS3Payload).isTrue()
        assertThat(config.ignorePayloadNotFound).isFalse()
        assertThat(config.s3KeyPrefix).isEmpty()
    }

    @Test
    fun `trims the s3 key prefix`() {
        val config = SqsExtendedClientConfig(payloadStore, s3KeyPrefix = "  my-prefix  ")

        assertThat(config.s3KeyPrefix).isEqualTo("my-prefix")
    }

    @Test
    fun `rejects an s3 key prefix that is too long`() {
        assertThatThrownBy { SqsExtendedClientConfig(payloadStore, s3KeyPrefix = "a".repeat(MAX_S3_KEY_PREFIX_LENGTH + 1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects an s3 key prefix starting with a dot`() {
        assertThatThrownBy { SqsExtendedClientConfig(payloadStore, s3KeyPrefix = ".hidden") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects an s3 key prefix starting with a slash`() {
        assertThatThrownBy { SqsExtendedClientConfig(payloadStore, s3KeyPrefix = "/absolute") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects an s3 key prefix containing a double dot`() {
        assertThatThrownBy { SqsExtendedClientConfig(payloadStore, s3KeyPrefix = "a/../b") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects an s3 key prefix with invalid characters`() {
        assertThatThrownBy { SqsExtendedClientConfig(payloadStore, s3KeyPrefix = "not valid!") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
