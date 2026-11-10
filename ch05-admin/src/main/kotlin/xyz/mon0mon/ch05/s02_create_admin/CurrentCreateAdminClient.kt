package xyz.mon0mon.ch05.s02_create_admin

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.DescribeClusterOptions
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.2 AdminClient 생성, 설정, 닫기
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.0  `Properties` + `AdminClient.create` + `close(Duration)` ([BookCreateAdminClient]).
 * - 2.5  `default.api.timeout.ms`가 API 전체의 대기 상한이 되었다. `request.timeout.ms`는 한 요청만 제한한다.
 * - 2.7  `Admin` 인터페이스. `Admin.create`가 진입점이다.
 * - 3.0  `close(long, TimeUnit)` 삭제.
 * - 4.4  `Admin.create(Map)`, `client.id`, `use`. `close()`만 쓰면 대기가 `Long.MAX_VALUE`라
 *        책의 30초 유예는 `close(Duration)`으로 그대로 둔다.
 *        `AdminClient` 클래스는 아직 동작하지만, 4.4 문서는 `Admin`을 쓰라고 한다.
 *
 * Replaces: [BookCreateAdminClient]
 * Removed: `AdminClient.close(long, TimeUnit)`
 * Still valid: `close(Duration)`, `request.timeout.ms`, `default.api.timeout.ms`
 *
 * 흐름:
 * 1. `Map`에 부트스트랩, `client.id`, 요청 타임아웃, API 타임아웃을 넣는다.
 * 2. `Admin.create`로 클라이언트를 만든다. 이 호출은 브로커에 요청을 보내지 않는다.
 * 3. `describeCluster`에 10초 옵션을 주고 클러스터 ID와 노드 수를 읽는다.
 *    이 호출은 연결 확인이다. 노드와 컨트롤러를 설명하는 절은 따로다.
 * 4. `close(Duration.ofSeconds(30))`로 남은 작업을 30초까지 기다린 뒤 닫는다.
 *    `use`가 끝나는 지점의 `close()`는 이미 닫힌 클라이언트에 한 번 더 호출된다.
 */
object CurrentCreateAdminClient {
    private const val CLIENT_ID = "ch05-s02-current"

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to CLIENT_ID,
            // 한 요청이 이 시간을 넘기면 재시도하거나 그 요청만 실패한다. 4.4 기본값과 같다.
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to 30_000,
            // get()이 재시도까지 포함해 기다릴 수 있는 상한. 4.4 기본값은 60초다.
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to 60_000,
        )
        // client.dns.lookup 기본값은 use_all_dns_ips다.
        // localhost는 이름 해석이 필요 없어서 키를 넣지 않는다.

        // region 5.2 AdminClient 생성, 설정, 닫기
        Admin.create(configs).use { admin ->
            try {
                val described = admin.describeCluster(DescribeClusterOptions().timeoutMs(10_000))
                val clusterId = described.clusterId().get()
                val nodeCount = described.nodes().get().size
                logger.info {
                    "admin ready clientId=$CLIENT_ID clusterId=$clusterId nodes=$nodeCount bootstrap=${KafkaSettings.bootstrapServers()}"
                }
            } catch (e: Exception) {
                logger.error(e) { "admin bootstrap failed" }
                throw e
            } finally {
                // use가 호출하는 close()는 유예가 Long.MAX_VALUE다.
                // 책의 30초 유예를 지키려고 여기서 먼저 닫는다.
                admin.close(Duration.ofSeconds(30))
            }
        }
        // endregion
    }
}
