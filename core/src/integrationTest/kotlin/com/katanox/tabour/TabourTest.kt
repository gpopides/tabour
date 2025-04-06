package com.katanox.tabour

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.CreateQueueRequest
import aws.sdk.kotlin.services.sqs.model.DeleteQueueRequest
import aws.sdk.kotlin.services.sqs.model.PurgeQueueRequest
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.katanox.tabour.configuration.core.DataProductionConfiguration
import com.katanox.tabour.configuration.core.tabour
import com.katanox.tabour.configuration.sqs.sqsConsumer
import com.katanox.tabour.configuration.sqs.sqsConsumerConfiguration
import com.katanox.tabour.configuration.sqs.sqsProducer
import com.katanox.tabour.configuration.sqs.sqsRegistry
import com.katanox.tabour.configuration.sqs.sqsRegistryConfiguration
import com.katanox.tabour.error.ProducerNotFound
import com.katanox.tabour.error.ProductionResourceNotFound
import com.katanox.tabour.error.RegistryNotFound
import com.katanox.tabour.sqs.production.FifoDataProduction
import com.katanox.tabour.sqs.production.NonFifoDataProduction
import com.katanox.tabour.sqs.production.SqsDataForProduction
import com.katanox.tabour.sqs.production.SqsDataProductionConfiguration
import com.katanox.tabour.sqs.production.SqsMessageProduced
import java.net.URI
import java.net.URL
import java.time.Duration
import kotlin.test.DefaultAsserter.assertNotNull
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.awaitility.kotlin.await
import org.awaitility.kotlin.withPollDelay
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName

@ExperimentalCoroutinesApi
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TabourTest {
    private val localstack =
        LocalStackContainer(DockerImageName.parse("localstack/localstack:4.2"))
            .withServices(LocalStackContainer.Service.SQS)
            .withReuse(true)

    private val credentials = StaticCredentialsProvider {
        accessKeyId = localstack.accessKey
        secretAccessKey = localstack.secretKey
    }
    private lateinit var sqsClient: SqsClient
    private lateinit var nonFifoQueueUrl: String
    private lateinit var fifoQueueUrl: String

    @BeforeAll
    fun setup() {
        localstack.start()

        sqsClient = SqsClient {
            credentialsProvider = StaticCredentialsProvider {
                accessKeyId = localstack.accessKey
                secretAccessKey = localstack.secretKey
            }
            endpointUrl =
                Url.parse(
                    localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()
                )
            region = localstack.region
        }

        nonFifoQueueUrl = runBlocking {
            sqsClient.createQueue(CreateQueueRequest { queueName = "my-queue" }).queueUrl ?: ""
        }

        fifoQueueUrl = runBlocking {
            sqsClient
                .createQueue(
                    CreateQueueRequest {
                        attributes =
                            mapOf(
                                QueueAttributeName.FifoQueue to "TRUE",
                                QueueAttributeName.ContentBasedDeduplication to "TRUE",
                            )
                        queueName = "my-queue.fifo"
                    }
                )
                .queueUrl ?: ""
        }
    }

    @AfterAll
    fun cleanup() {
        runBlocking {
            sqsClient.deleteQueue(DeleteQueueRequest { queueUrl = nonFifoQueueUrl })
            sqsClient.deleteQueue(DeleteQueueRequest { queueUrl = fifoQueueUrl })
        }
    }

    @Test
    fun isRunningIsTrue() = runTest {
        val container = tabour { numOfThreads = 1 }
        container.start()
        assertTrue { container.running() }
        container.stop()
    }

    @Test
    fun isRunningIsFalse() = runTest {
        val container = tabour { numOfThreads = 1 }
        container.start()
        container.stop()
        assertFalse { container.running() }
    }

    @Test
    @Tag("sqs-consumer-test")
    fun `consume messages`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            var counter = 0
            val sqsProducerConfiguration =
                DataProductionConfiguration<SqsDataForProduction, SqsMessageProduced>(
                    produceData = { NonFifoDataProduction("this is a test message") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { _ -> println("Resource not found") },
                )

            val producer =
                sqsProducer(URL.of(URI.create(nonFifoQueueUrl), null), "test-producer", ::println)

            val consumer =
                sqsConsumer(
                    URL.of(URI.create(nonFifoQueueUrl), null),
                    key = "my-consumer",
                    onSuccess = {
                        counter++
                        true
                    },
                    onError = ::println,
                ) {
                    this.config = sqsConsumerConfiguration {
                        sleepTime = 200.milliseconds
                        consumeWhile = { counter < 1 }
                    }
                }

            sqsRegistry.addConsumer(consumer).addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage("test-registry", "test-producer", sqsProducerConfiguration)

            await.withPollDelay(Duration.ofSeconds(3)).untilAsserted { assertTrue { counter >= 1 } }

            purgeQueue(nonFifoQueueUrl)
            container.stop()
        }

    @Test
    @Tag("sqs-consumer-test")
    fun `consume messages with multi concurrency`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            var counter = 0

            val producer =
                sqsProducer(URL.of(URI.create(nonFifoQueueUrl), null), "test-producer") {
                    println(it)
                }
            val consumer =
                sqsConsumer(
                    URL.of(URI.create(nonFifoQueueUrl), null),
                    key = "my-consumer",
                    onSuccess = {
                        counter++
                        true
                    },
                    onError = ::println,
                ) {
                    this.config = sqsConsumerConfiguration {
                        sleepTime = 200.milliseconds
                        consumeWhile = { counter < 50 }
                        concurrency = 5
                        maxMessages = 2
                    }
                }

            sqsRegistry.addConsumer(consumer).addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { NonFifoDataProduction("this is a test message") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { _ -> println("Resource not found") },
                )

            repeat(50) {
                container.produceMessage("test-registry", "test-producer", sqsProducerConfiguration)
            }

            await.withPollDelay(Duration.ofSeconds(2)).untilAsserted { assertTrue { counter > 1 } }
            container.stop()
        }

    @Test
    @Tag("sqs-producer-test")
    fun `produce a message to a non fifo queue`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { NonFifoDataProduction("this is a test message") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { _ -> println("Resource not found") },
                )

            val producer =
                sqsProducer(URL.of(URI.create(nonFifoQueueUrl), null), "test-producer", ::println)

            sqsRegistry.addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage("test-registry", "test-producer", sqsProducerConfiguration)

            await
                .withPollInterval(Duration.ofMillis(500))
                .timeout(Duration.ofSeconds(5))
                .untilAsserted {
                    val receiveMessagesResponse = runBlocking {
                        sqsClient.receiveMessage(
                            ReceiveMessageRequest {
                                queueUrl = nonFifoQueueUrl
                                maxNumberOfMessages = 5
                            }
                        )
                    }

                    assertTrue(receiveMessagesResponse.messages?.isNotEmpty() == true)
                    assertEquals(
                        receiveMessagesResponse.messages?.first()?.body,
                        "this is a test message",
                    )
                }

            purgeQueue(nonFifoQueueUrl)
            container.stop()
        }

    @Test
    @Tag("sqs-producer-test")
    fun `produce a message to a fifo queue`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { FifoDataProduction("this is a fifo test message", "group1") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { _ -> println("Resource not found") },
                )

            val producer =
                sqsProducer(URL.of(URI.create(fifoQueueUrl), null), "fifo-test-producer") {}

            sqsRegistry.addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage(
                "test-registry",
                "fifo-test-producer",
                sqsProducerConfiguration,
            )

            await
                .withPollInterval(Duration.ofMillis(500))
                .timeout(Duration.ofSeconds(5))
                .untilAsserted {
                    val receiveMessagesResponse = runBlocking {
                        sqsClient.receiveMessage(
                            ReceiveMessageRequest {
                                queueUrl = fifoQueueUrl
                                maxNumberOfMessages = 5
                            }
                        )
                    }
                    println(receiveMessagesResponse.messages)

                    assertTrue(receiveMessagesResponse.messages?.isNotEmpty() == true)
                    assertEquals(
                        receiveMessagesResponse.messages?.firstOrNull()?.body,
                        "this is a fifo test message",
                    )
                }
            purgeQueue(fifoQueueUrl)
            container.stop()
        }

    @Test
    @Tag("sqs-producer-test")
    fun `successful production triggers dataProduced function`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            var expectedProduceData: SqsDataForProduction? = null
            var producedDataEvent: SqsMessageProduced? = null

            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { FifoDataProduction("this is a fifo test message", "group1") },
                    dataProduced = { data, event ->
                        expectedProduceData = data
                        producedDataEvent = event
                    },
                    resourceNotFound = { _ -> println("Resource not found") },
                )

            val producer =
                sqsProducer(URL.of(URI.create(fifoQueueUrl), null), "fifo-test-producer", ::println)

            sqsRegistry.addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage(
                "test-registry",
                "fifo-test-producer",
                sqsProducerConfiguration,
            )

            purgeQueue(fifoQueueUrl)

            await.withPollDelay(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(
                    FifoDataProduction("this is a fifo test message", "group1"),
                    expectedProduceData,
                )
                assertNotNull("Message group id is null", producedDataEvent?.messageGroupId)
                assertNotEquals("", producedDataEvent?.messageGroupId)
            }
            container.stop()
        }

    @Test
    @Tag("sqs-producer-test")
    fun `produce a message with wrong registry key triggers resource not found error`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            var resourceNotFound: ProductionResourceNotFound? = null
            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { FifoDataProduction("this is a fifo test message", "group1") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { error -> resourceNotFound = error },
                )

            val producer =
                sqsProducer(URL.of(URI.create(fifoQueueUrl), null), "fifo-test-producer") {
                    println(it)
                }

            sqsRegistry.addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage(
                "wrong-registry",
                "fifo-test-producer",
                sqsProducerConfiguration,
            )

            await.withPollDelay(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(RegistryNotFound("wrong-registry"), resourceNotFound)
            }
            container.stop()
        }

    @Test
    @Tag("sqs-producer-test")
    fun `produce a message with wrong producer key triggers resource not found error`() =
        runTest(UnconfinedTestDispatcher()) {
            val container = tabour { numOfThreads = 1 }
            val config =
                sqsRegistryConfiguration("test-registry", credentials, localstack.region) {
                    endpointOverride =
                        localstack.getEndpointOverride(LocalStackContainer.Service.SQS)
                }

            val sqsRegistry = sqsRegistry(config)
            var resourceNotFound: ProductionResourceNotFound? = null
            val sqsProducerConfiguration =
                SqsDataProductionConfiguration(
                    produceData = { FifoDataProduction("this is a fifo test message", "group1") },
                    dataProduced = { _, _ -> },
                    resourceNotFound = { error -> resourceNotFound = error },
                )

            val producer =
                sqsProducer(URL.of(URI.create(fifoQueueUrl), null), "fifo-test-producer", ::println)

            sqsRegistry.addProducer(producer)
            container.register(sqsRegistry)
            container.start()

            container.produceMessage("test-registry", "wrong-producer", sqsProducerConfiguration)

            await.withPollDelay(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(ProducerNotFound("wrong-producer"), resourceNotFound)
            }

            container.stop()
        }

    private fun purgeQueue(url: String) {
        runBlocking { sqsClient.purgeQueue(PurgeQueueRequest { queueUrl = url }) }
    }
}
