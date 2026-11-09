package xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.3 Asynchronous Commit — retrying commitAsync
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  커밋마다 단조 증가 시퀀스를 콜백에 붙인다. 실패했을 때 그 시퀀스가 아직 최신이면 다시 보내고,
 *        더 큰 시퀀스가 있으면 오래된 재시도를 버린다 (책 2판, 설명만 있음).
 * - 4.4  `Map` + deserializer 인스턴스, `use`, SAM 콜백.
 *        재시도는 그 호출의 오프셋 스냅샷만 다시 보낸다. 같은 실패가 프로세스를 붙잡지 않게 횟수를 제한한다.
 *        콜백 없는 커밋은 [CurrentAsyncCommit], 재시도 없는 콜백은 [CurrentAsyncCommitCallback].
 *
 * Still valid: `commitAsync(Map, OffsetCommitCallback)`, `poll(Duration)`, `committed(Set, Duration)`
 *
 * 흐름 (샘플 3건이 두 번의 poll로 나뉜 경우):
 * 1. 토픽을 만들고 파티션을 받은 뒤에 샘플을 넣는다. 그다음 poll로 읽는다.
 * 2. 첫 poll이 오프셋 0을 준다. lastProcessed에는 0, currentOffsets에는 다음에 읽을 위치인 1을 넣는다.
 *    커밋 오프셋은 처리한 오프셋 + 1이다.
 * 3. 레코드가 있으므로 commitInOrder를 호출한다. sequence가 0에서 1이 되고, 그 1이 이 호출의 seq로 남는다.
 *    맵은 snapshot으로 복사한 뒤 commitAsync(snapshot)으로 보낸다. 호출은 브로커 응답을 기다리지 않고 반환된다.
 *    이 호출의 람다만 그 seq를 캡처한다. 다음 호출의 seq는 다른 변수다.
 * 4. 다음 poll이 오프셋 1과 2를 준다. 이때 1번 응답은 아직 없어서 콜백은 돌지 않는다.
 *    currentOffsets는 3으로 갱신되고, commitInOrder가 한 번 더 호출되어 그 호출의 seq는 2가 된다.
 * 5. 3건을 다 보면 루프가 끝난다. awaitCommitted의 poll에서 밀려 있던 콜백이 실행된다.
 *    콜백은 성공이든 실패든 이때 돈다. seq=1은 오프셋 1, seq=2는 오프셋 3이다.
 * 6. 그룹에 남은 값은 3이다. 마지막으로 처리한 레코드 2의 다음 위치와 같다.
 *
 * 1번 콜백이 돌 때 sequence.get()은 이미 2다. 성공이면 비교 전에 반환한다.
 * 실패인데 seq != sequence.get()이면 1번 스냅샷은 다시 보내지 않는다. 오프셋 1이 이미 보낸 3을 되돌릴 수 있기 때문이다.
 * 번호가 아직 최신이면 같은 snapshot만 다시 보내며, 재시도도 sequence를 1 올린다. 시도는 MAX_ATTEMPTS까지다.
 * poll이 레코드를 어떻게 묶는지는 실행마다 달라질 수 있다. 어긋나는 지점은 같다. 앞 커밋의 콜백보다 다음 커밋이 먼저 나간다.
 */
object CurrentAsyncCommitRetry {
    private const val TOPIC = "ch04OffsetCommit"
    private const val GROUP = "CountryCounterCommit"
    private const val MAX_ATTEMPTS = 3

    @JvmStatic
    fun main(args: Array<String>) {
        // 1. 토픽을 보장하고, 파티션을 받은 뒤에 샘플을 넣는다.
        //    구독 전에 보내면 이 그룹은 그 레코드를 읽지 못할 수 있다.
        ensureTopic(TOPIC)

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s063-retry-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s063-retry-current",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        )

        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                consumer.subscribe(listOf(TOPIC))
                awaitAssignment(consumer)
                if (consumer.assignment().isEmpty()) {
                    return
                }

                val samples = listOf(
                    "Precision Products" to "France",
                    "Biomedical Materials" to "USA",
                    "Precision Products" to "France",
                )
                for ((customer, country) in samples) {
                    try {
                        val metadata = producer.send(ProducerRecord(TOPIC, customer, country)).get()
                        logger.info {
                            "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                        }
                    } catch (e: Exception) {
                        logger.error(e) { "send failed" }
                    }
                }

                // region 4.6.3 비동기적 커밋 재시도하기
                // 2. 배치를 읽을 때마다 commitAsync로 오프셋을 보낸다. 응답은 기다리지 않는다.
                //    콜백은 성공이든 실패든 이후 poll() 안에서 실행된다.
                //
                // sequence는 이 루프가 공유하는 카운터다. commitInOrder가 불릴 때마다 1 증가한다.
                // seq는 그 호출이 incrementAndGet()에서 받은 값이고, 그 호출의 람다만 캡처한다.
                // 다음 호출의 seq는 다른 변수다. 첫 람다는 1, 둘째 람다는 2를 끝까지 기억한다.
                //
                // 1번 응답이 오기 전에 2번 커밋이 나가면, 늦은 1번 콜백의 seq는 1이고
                // sequence.get()은 2다. 그때 1번 스냅샷을 다시 보내면 더 큰 오프셋을 되돌릴 수 있다.
                val sequence = AtomicInteger()
                val callbackSeen = AtomicInteger()
                // currentOffsets의 값은 처리한 오프셋 + 1, 즉 다음에 읽을 위치다.
                val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
                val lastProcessed = HashMap<TopicPartition, Long>()
                val deadline = System.currentTimeMillis() + 10_000
                var commits = 0
                var seen = 0

                fun commitInOrder(offsets: Map<TopicPartition, OffsetAndMetadata>, attempt: Int = 1) {
                    // 루프가 currentOffsets를 이어서 고쳐도, 이 호출의 재시도 내용은 바뀌지 않는다.
                    val snapshot = HashMap(offsets)
                    // 이번 호출 전용 번호. 람다가 이 val을 캡처해 콜백 시점까지 유지한다.
                    val seq = sequence.incrementAndGet()
                    consumer.commitAsync(snapshot) { committed, exception ->
                        callbackSeen.incrementAndGet()
                        if (exception == null) {
                            logger.info { "commit callback seq=$seq attempt=$attempt offsets=$committed" }
                            return@commitAsync
                        }
                        // 이 콜백보다 새로운 커밋이 이미 나갔다. 재시도를 버린다.
                        if (seq != sequence.get()) {
                            logger.error(exception) {
                                "skip retry seq=$seq latest=${sequence.get()} offsets=$committed"
                            }
                            return@commitAsync
                        }
                        // 번호가 아직 최신이다. 같은 스냅샷만 다시 보낸다.
                        // 재시도도 sequence를 1 올리므로, 그보다 늦은 오래된 콜백은 위의 비교에서 걸러진다.
                        // 책은 횟수 제한이 없다. 같은 실패가 계속될 때 여기서 멈추도록 상한만 두었다.
                        if (attempt >= MAX_ATTEMPTS) {
                            logger.error(exception) {
                                "commit failed seq=$seq attempts=$attempt offsets=$committed"
                            }
                            return@commitAsync
                        }
                        logger.error(exception) {
                            "retry commit seq=$seq attempt=$attempt offsets=$committed"
                        }
                        commitInOrder(snapshot, attempt + 1)
                    }
                }

                while (seen < samples.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(100))
                    for (record in records) {
                        logger.info {
                            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                        }
                        val tp = TopicPartition(record.topic(), record.partition())
                        lastProcessed[tp] = record.offset()
                        currentOffsets[tp] = OffsetAndMetadata(record.offset() + 1)
                        seen++
                    }
                    if (records.isEmpty) {
                        continue
                    }
                    // 반환 직후 콜백은 돌지 않는다. 다음 배치의 커밋이 이전 콜백보다 먼저 나갈 수 있다.
                    commitInOrder(currentOffsets)
                    commits++
                }
                if (seen < samples.size) {
                    logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                }
                // 3. 밀려 있던 콜백은 여기 poll()에서 실행된다.
                //    저장된 오프셋이 마지막 레코드 + 1인지 확인한다.
                awaitCommitted(consumer, lastProcessed, callbackSeen, commits)
                logCommitted(consumer, lastProcessed)
                logGroupOffsets(GROUP)
                if (callbackSeen.get() < commits) {
                    logger.error { "commit callback missing seen=${callbackSeen.get()} commits=$commits" }
                }
                // endregion
            }
        }
    }

    private fun ensureTopic(topic: String) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val names = admin.listTopics().names().get()
            if (names.contains(topic)) {
                val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get()
                    .getValue(topic).partitions().size
                logger.info { "topic exists name=$topic partitions=$partitions" }
                return
            }
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
            logger.info { "topic created name=$topic partitions=1" }
        }
    }

    private fun awaitAssignment(consumer: KafkaConsumer<String, String>) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().isEmpty()) {
            logger.error { "assignment timed out group=$GROUP topic=$TOPIC" }
        }
    }

    private fun awaitCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
        callbackSeen: AtomicInteger,
        commits: Int,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
            val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
            val offsetsMatch = lastProcessed.all { (tp, processed) ->
                stored[tp]?.offset() == processed + 1
            }
            if (offsetsMatch && callbackSeen.get() >= commits) {
                return
            }
        }
        logger.error { "async commit timed out callbacks=${callbackSeen.get()} commits=$commits" }
    }

    private fun logCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
    ) {
        val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
        for (tp in consumer.assignment()) {
            val committedOffset = stored[tp]?.offset()
            val processed = lastProcessed[tp]
            logger.info {
                "committed topic=${tp.topic()} partition=${tp.partition()} offset=$committedOffset lastProcessed=$processed"
            }
            if (processed != null && committedOffset != processed + 1) {
                logger.error {
                    "commit mismatch partition=${tp.partition()} expected=${processed + 1} committed=$committedOffset"
                }
            }
        }
    }

    private fun logGroupOffsets(group: String) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
            val topicOffsets = offsets.filterKeys { it.topic() == TOPIC }
            if (topicOffsets.isEmpty()) {
                logger.error { "group offset missing group=$group topic=$TOPIC" }
                return
            }
            for ((tp, meta) in topicOffsets) {
                logger.info {
                    "group=$group topic=${tp.topic()} partition=${tp.partition()} committed=${meta.offset()}"
                }
            }
        }
    }
}
