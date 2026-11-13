package xyz.mon0mon.ch05.s08_testing

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.MockAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Book: §5.8 테스트하기
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.5  `MockAdminClient.incrementalAlterConfigs`는 `UnsupportedOperationException`이었다.
 *        책은 Mockito spy로 그 호출만 비우고, `createTopics`가 불렸는지만 본다.
 * - 2.6+ 같은 클래스가 SET과 DELETE를 메모리에 반영한다. `listTopics`로 만든 토픽이 보인다.
 * - 4.4  `MockAdminClient`는 공개 jar가 아니다. `kafka-clients`의 `test-fixtures` 분류에 있다.
 *        `new MockAdminClient(brokers, controller)`는 그대로다.
 *
 * Still valid: `MockAdminClient(List<Node>, Node)`, `createTopics`, `incrementalAlterConfigs`, `listTopics`
 *
 * 흐름:
 * 1. 브로커 노드 하나로 `MockAdminClient`를 만든다. 네트워크는 없다.
 * 2. 이름이 `test`로 시작하면 파티션 1, 복제 1 토픽을 만들고 `cleanup.policy=compact`로 SET한다.
 * 3. `test`로 시작하지 않으면 Admin 호출을 하지 않는다.
 * 4. `listTopics`와 `describeConfigs`로 그 결과를 확인한다. 브로커는 띄우지 않는다.
 */
class BookTopicCreator(
    private val admin: AdminClient,
) {
    /**
     * 이름이 `test`로 시작할 때만 토픽을 만든다.
     * 책은 `createTopics`의 future를 기다리지 않는다. 이 목은 반환 전에 future를 끝내므로 다음 alter가 토픽을 본다.
     */
    fun maybeCreateTopic(topicName: String) {
        val topics = mutableListOf(NewTopic(topicName, 1, 1.toShort()))
        if (topicName.lowercase().startsWith("test")) {
            // region 5.8 테스트하기
            admin.createTopics(topics)

            val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
            val compaction = ConfigEntry(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT,
            )
            val ops = listOf(AlterConfigOp(compaction, AlterConfigOp.OpType.SET))
            // 4.4: 책은 2.5 목이라 이 호출을 spy로 비웠다. 4.4 목은 SET을 저장하므로 get()으로 끝낸다.
            admin.incrementalAlterConfigs(mapOf(resource to ops)).all().get()
            // endregion
        }
    }
}

class BookAdminClientTest {
    @Test
    fun `test로 시작하면 토픽을 만들고 cleanup policy를 compact로 둔다`() {
        val broker = Node(0, "localhost", 9092)
        // 컨트롤러는 브로커 목록 안에 있어야 한다. 책은 노드 하나를 둘 다에 넣는다.
        val admin = MockAdminClient(listOf(broker), broker)
        try {
            BookTopicCreator(admin).maybeCreateTopic("test.is.a.test.topic")

            val names = admin.listTopics().names().get()
            assertTrue(names.contains("test.is.a.test.topic"))
            assertEquals(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                cleanupPolicy(admin, "test.is.a.test.topic"),
            )
        } finally {
            admin.close()
        }
    }

    @Test
    fun `test로 시작하지 않으면 토픽을 만들지 않는다`() {
        val broker = Node(0, "localhost", 9092)
        val admin = MockAdminClient(listOf(broker), broker)
        try {
            BookTopicCreator(admin).maybeCreateTopic("not.a.test")

            assertFalse(admin.listTopics().names().get().contains("not.a.test"))
        } finally {
            admin.close()
        }
    }

    private fun cleanupPolicy(admin: AdminClient, topic: String): String {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        val config = admin.describeConfigs(listOf(resource)).all().get().getValue(resource)
        return config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value()
    }
}
