package com.katanox.tabour.sqs.production

import com.katanox.tabour.plug.FailurePlugRecord
import com.katanox.tabour.plug.SuccessPlugRecord
import com.katanox.tabour.retry
import java.time.Instant
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

internal class SqsProducerExecutor(private val sqs: SqsClient) {
    suspend fun <T> produce(
        producer: SqsProducer<T>,
        productionConfiguration: SqsDataProductionConfiguration
    ) {
        val produceData = productionConfiguration.produceData()

        val url = producer.queueUrl.toString()

        if (url.isEmpty()) {
            producer.onError(ProductionError.EmptyUrl(producer.queueUrl))
            return
        }

        val throwableToError: (Throwable) -> ProductionError = {
            when (it) {
                is AwsServiceException -> ProductionError.AwsError(details = it.awsErrorDetails())
                is SdkClientException -> ProductionError.AwsSdkClientError(it)
                else -> ProductionError.UnrecognizedError(it)
            }
        }

        when (produceData) {
            is SqsDataForProduction -> {
                if (!produceData.message.isNullOrEmpty()) {
                    retry(
                        producer.config.retries,
                        {
                            val error = throwableToError(it)

                            producer.onError(error)
                            producer.notifyPlugs(produceData.message, error)
                        }
                    ) {
                        sqs.sendMessage {
                                produceData.buildMessageRequest(it)
                                it.queueUrl(url)
                            }
                            .let { response ->
                                if (response.messageId().isNotEmpty()) {
                                    productionConfiguration.dataProduced(
                                        produceData,
                                        SqsMessageProduced(response.messageId(), Instant.now())
                                    )

                                    producer.notifyPlugs(produceData.message)
                                }
                            }
                    }
                } else {
                    if (produceData.message.isNullOrEmpty()) {
                        producer.onError(ProductionError.EmptyMessage(produceData))
                        producer.notifyPlugs(
                            produceData.message,
                            ProductionError.EmptyMessage(produceData)
                        )
                    }
                }
            }
            is BatchSqsData -> {
                if (produceData.data.isNotEmpty()) {
                    retry(producer.config.retries, { producer.onError(throwableToError(it)) }) {
                        val response =
                            sqs.sendMessageBatch {
                                produceData.buildMessageRequest(it)
                                it.queueUrl(url)
                            }

                        response.failed().forEach {
                            producer.notifyPlugs(
                                it.message(),
                                throwableToError(RuntimeException(it.message()))
                            )
                        }
                    }
                }
            }
        }
    }

    private suspend fun <T> SqsProducer<T>.notifyPlugs(
        message: String?,
        error: ProductionError? = null
    ) {
        if (plugs.isNotEmpty()) {
            plugs.forEach { plug ->
                if (error == null) {
                    plug.handle(SuccessPlugRecord(message, key))
                } else {
                    plug.handle(FailurePlugRecord(message, key, error))
                }
            }
        }
    }
}

private fun SqsDataForProduction.buildMessageRequest(builder: SendMessageRequest.Builder) {
    when (this) {
        is FifoQueueData -> {
            builder.messageBody(message)
            builder.messageGroupId(messageGroupId)

            if (messageDeduplicationId != null) {
                builder.messageDeduplicationId(messageDeduplicationId)
            }
        }
        is NonFifoQueueData -> builder.messageBody(message)
    }
}

private fun SqsDataForProduction.buildMessageRequest(
    builder: SendMessageBatchRequestEntry.Builder
) {
    when (this) {
        is FifoQueueData -> {
            builder.messageBody(message)
            builder.messageGroupId(messageGroupId)

            if (messageDeduplicationId != null) {
                builder.messageDeduplicationId(messageDeduplicationId)
            }
        }
        is NonFifoQueueData -> builder.messageBody(message)
    }
}

private fun BatchSqsData.buildMessageRequest(builder: SendMessageBatchRequest.Builder) {
    builder.entries(
        data.map {
            val entryBuilder = SendMessageBatchRequestEntry.builder()
            it.buildMessageRequest(entryBuilder)
            entryBuilder.build()
        }
    )
}
