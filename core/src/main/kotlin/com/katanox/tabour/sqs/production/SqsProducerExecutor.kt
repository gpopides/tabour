package com.katanox.tabour.sqs.production

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import com.katanox.tabour.retry
import java.time.Instant

internal class SqsProducerExecutor(private val sqs: SqsClient) {
    suspend fun <T> produce(
        producer: SqsProducer<T>,
        productionConfiguration: SqsDataProductionConfiguration,
    ) {
        val produceData = productionConfiguration.produceData()

        val url = producer.queueUrl.toString()

        if (url.isEmpty()) {
            producer.onError(ProductionError.EmptyUrl(producer.queueUrl))
            return
        }

        val throwableToError: (Throwable) -> ProductionError = {
            when (it) {
                //                is AwsServiceException -> ProductionError.AwsError(details =
                // it.awsErrorDetails())
                //                is SdkClientException -> ProductionError.AwsSdkClientError(it)
                else -> ProductionError.UnrecognizedError(it)
            }
        }

        when (produceData) {
            is SqsProductionData -> {
                if (!produceData.message.isNullOrEmpty()) {
                    retry(producer.config.retries, { producer.onError(throwableToError(it)) }) {
                        sqs.sendMessage(
                                SendMessageRequest {
                                    produceData.buildMessageRequest(this)
                                    queueUrl = url
                                }
                            )
                            .let { response ->
                                response.messageId?.also { messageId ->
                                    if (messageId.isNotEmpty()) {
                                        if (response.messageId?.isNotEmpty() == true)
                                            productionConfiguration.dataProduced?.invoke(
                                                produceData,
                                                SqsMessageProduced(messageId, Instant.now()),
                                            )
                                    }
                                }
                            }
                    }
                } else {
                    if (produceData.message.isNullOrEmpty()) {
                        producer.onError(ProductionError.EmptyMessage(produceData))
                    }
                }
            }
            is BatchDataForProduction -> {
                if (produceData.data.isNotEmpty()) {
                    produceData.data
                        .chunked(10) {
                            aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequest {
                                queueUrl = url
                                entries = buildBatchGroup(it)
                            }

                            aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequest {
                                queueUrl = url
                                entries = buildBatchGroup(it)
                            }
                        }
                        .forEach { request ->
                            retry(
                                producer.config.retries,
                                { producer.onError(throwableToError(it)) },
                            ) {
                                val response = sqs.sendMessageBatch(request)

                                if (response.failed.isEmpty()) {
                                    response.successful.zip(produceData.data).forEach {
                                        (entry, data) ->
                                        productionConfiguration.dataProduced?.invoke(
                                            data,
                                            SqsMessageProduced(entry.messageId, Instant.now()),
                                        )
                                    }
                                }
                            }
                        }
                }
            }
        }
    }
}

private fun SqsProductionData.buildMessageRequest(builder: SendMessageRequest.Builder) {
    when (this) {
        is FifoDataProduction -> {
            builder.messageBody = message
            builder.messageGroupId = messageGroupId

            if (messageDeduplicationId != null) {
                builder.messageDeduplicationId = messageDeduplicationId
            }
        }
        is NonFifoDataProduction -> builder.messageBody = message
    }
}

private fun SqsProductionData.buildMessageRequest(builder: SendMessageBatchRequestEntry.Builder) {
    when (this) {
        is FifoDataProduction -> {
            builder.messageBody = "message"
            builder.messageGroupId = messageGroupId

            if (messageDeduplicationId != null) {
                builder.messageDeduplicationId = messageDeduplicationId
            }
        }
        is NonFifoDataProduction -> builder.messageBody = message
    }
}

private fun buildBatchGroup(data: List<SqsProductionData>): List<SendMessageBatchRequestEntry> =
    data.mapIndexed { i, it ->
        SendMessageBatchRequestEntry {
            it.buildMessageRequest(this)
            this.id = (i + 1).toString()
        }
    }
