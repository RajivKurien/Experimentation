import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.PatternLayout
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsResult
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.ConfigResource
import org.h2.jdbcx.JdbcDataSource
import org.http4k.core.HttpHandler
import org.http4k.core.Method.GET
import org.http4k.core.Method.POST
import org.http4k.core.Response
import org.http4k.core.Status.Companion.OK
import org.http4k.routing.bind
import org.http4k.routing.path
import org.http4k.routing.routes
import org.http4k.server.SunHttp
import org.http4k.server.asServer
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

fun main() {
    logToConsole()
    val kafkaBootstrapServers = "localhost:9092"
    val httpPort = 8000

    val dataSource = setupDatabase()

    val adminClient = AdminClient.create(properties(kafkaBootstrapServers))
    val producer = createKafkaProducer(kafkaBootstrapServers)
    val consumer = createKafkaConsumer(kafkaBootstrapServers)

    Thread {
        while (true) {
            logger.info { "Polling" }
            pollKafkaMessages(adminClient, consumer, dataSource)
            Thread.sleep(1000)
        }
    }.start()


    val app: HttpHandler = routes(
        "/topics/{topicName}/send" bind POST to { request ->
            val topic = request.path("topicName") ?: throw IllegalArgumentException("Topic name is required")
            val message = request.body.stream.readBytes()
            logger.info { "Sending message:$message to topic:$topic " }
            producer.send(ProducerRecord(topic, message))
            Response(OK)
        },

        "/topics/{topicName}/messages" bind GET to { request ->
            val topic = request.path("topicName") ?: throw IllegalArgumentException("Topic name is required")
            val messages = getMessagesFromDatabase(dataSource, topic)
            Response(OK).body(messages.joinToString("\n"))
        },

        "/topics" bind GET to {
            val topics = getTopics(adminClient)
            Response(OK).body(topics.joinToString("\n"))
        },

        "/broker/config" bind GET to {
            val brokerConfig = getBrokerConfig(adminClient)
            Response(OK).body(brokerConfig.joinToString("\n"))
        })

    println("Server running on port $httpPort")
    app.asServer(SunHttp(httpPort)).start().block()
}

private fun createKafkaProducer(kafkaBootstrapServers: String): KafkaProducer<String, ByteArray> {
    return KafkaProducer(
        mapOf(
            "bootstrap.servers" to kafkaBootstrapServers,
            "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
            "security.protocol" to "PLAINTEXT"
        )
    )
}

private fun createKafkaConsumer(kafkaBootstrapServers: String): KafkaConsumer<String, ByteArray> {
    return KafkaConsumer(
        mapOf(
            "bootstrap.servers" to kafkaBootstrapServers,
            "group.id" to "my-group",
            "client.id" to "kafka-consumer",
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        )
    )
}

private fun properties(bootstrapServers: String): Properties {
    return Properties().apply {
        put("bootstrap.servers", bootstrapServers)
    }
}

private fun getTopics(adminClient: AdminClient): List<String> {
    val options = ListTopicsOptions().listInternal(false)
    val listTopicsResult: ListTopicsResult = adminClient.listTopics(options)
    val topics = listTopicsResult.names().get().toList()
    logger.info { "Topics:{$topics}" }
    return topics
}

private fun getBrokerConfig(adminClient: AdminClient): List<String> {
    val describeClusterResult = adminClient.describeCluster(DescribeClusterOptions())
    val nodes = describeClusterResult.nodes().get()

    val brokerConfigsFuture = adminClient.describeConfigs(nodes.map { node ->
        ConfigResource(ConfigResource.Type.BROKER, node.idString())
    }).all()

    val brokerConfigs = brokerConfigsFuture.get()

    return brokerConfigs.entries.flatMap { entry ->
        entry.value.entries().map { configEntry ->
            "${configEntry.name()}=${configEntry.value()}"
        }
    }
}

fun pollKafkaMessages(
    adminClient: AdminClient, consumer: KafkaConsumer<String, ByteArray>, dataSource: JdbcDataSource
) {
    synchronized(consumer) {
        consumer.subscribe((getTopics(adminClient)))
        consumer.poll(100.milliseconds.toJavaDuration()).forEach { record ->
            val topic = record.topic()
            val message = String(record.value())
            logger.info { "Received message: ${message}" }
            saveMessageToDatabase(dataSource, topic, message)
        }
    }
}

fun setupDatabase(): JdbcDataSource {
    val dataSource = JdbcDataSource()
    dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
    dataSource.user = "sa"
    dataSource.password = ""
    DriverManager.getConnection(dataSource.getURL(), dataSource.user, dataSource.password).use { connection ->
        val statement = connection.createStatement()
        statement.execute("CREATE TABLE IF NOT EXISTS messages (id INT AUTO_INCREMENT PRIMARY KEY, topic VARCHAR(255), message VARCHAR(255))")
    }
    return dataSource
}

fun saveMessageToDatabase(dataSource: JdbcDataSource, topic: String, message: String) =
    runCatching {
        logger.info { "Saving to database.\n Topic:{$topic}, Message:{$message}" }
        dataSource.connection.use { connection ->
            val statement: Statement = connection.createStatement()
            val sql = "INSERT INTO messages (topic, message) VALUES ('$topic', '$message')"
            statement.executeUpdate(sql)
        }
    }.onFailure { exception ->
        logger.error(exception) { "Error saving message to database: ${exception.message}" }
    }


fun getMessagesFromDatabase(dataSource: JdbcDataSource, topic: String): List<String> = runCatching {
    logger.info { "Reading from database for \n Topic:{$topic}" }
    val messages = mutableListOf<String>()
    dataSource.connection.use { connection ->
        connection.createStatement().use { statement ->
            val resultSet = statement.executeQuery("SELECT * FROM messages WHERE topic='$topic'")
            while (resultSet.next()) {
                val message = resultSet.getString("message")
                messages.add(message)
            }
        }
    }
    messages
}.getOrElse { exception ->
    logger.error(exception) { "Error reading messages from database: ${exception.message}" }
    emptyList()
}

fun logToConsole() {
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    rootLogger.level = Level.INFO

    val context = LoggerFactory.getILoggerFactory() as LoggerContext

    val layout = PatternLayout().apply {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        this.context = context
        start()
    }

    val encoder = LayoutWrappingEncoder<ILoggingEvent>().apply {
        this.layout = layout
        this.context = context
        start()
    }

    val consoleAppender = ConsoleAppender<ILoggingEvent>().apply {
        name = "CONSOLE"
        this.encoder = encoder
        this.context = context
        start()
    }

    rootLogger.addAppender(consoleAppender)
}
