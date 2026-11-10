package xyz.mon0mon.ch05.s02_create_admin

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.2 AdminClient 생성, 설정, 닫기
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `AdminClient.create(Properties)`. 초기 `close()`는 대기 시간을 받지 않았다.
 * - 2.0  `close(Duration)`. 진행 중인 요청은 그 시간만큼 기다린 뒤 남은 작업을 끊는다 (책 2판, 이 파일).
 * - 2.5  `default.api.timeout.ms` (KIP-533). 한 요청의 상한은 `request.timeout.ms`,
 *        재시도까지 포함한 API 상한은 `default.api.timeout.ms`. 책 2판이 설명하는 대기 상한은 앞의 키다.
 *        2.5부터 `request.timeout.ms` 기본값은 120초에서 30초로 줄었다.
 * - 3.0  `close(long, TimeUnit)` 삭제. `close(Duration)`만 남았다.
 * - 4.4  `AdminClient.create`는 그대로 동작한다. 진입점은 `Admin.create`가 권장된다
 *        → [CurrentCreateAdminClient].
 *
 * Removed: `AdminClient.close(long, TimeUnit)`
 * Still valid: `AdminClient.create(Properties)`, `close(Duration)`, `request.timeout.ms`
 *
 * 흐름:
 * 1. `bootstrap.servers`와 `request.timeout.ms`를 `Properties`에 넣는다.
 * 2. `AdminClient.create`로 클라이언트를 만든다. 이 호출은 브로커에 요청을 보내지 않는다.
 * 3. `describeCluster`로 클러스터 ID를 읽어 부트스트랩이 붙었는지 확인한다.
 *    노드와 컨트롤러를 읽는 본문은 뒤 절이다.
 * 4. `close(Duration.ofSeconds(30))`로 남은 작업을 30초까지 기다린 뒤 닫는다.
 *    닫은 뒤에는 새 요청을 보내지 않는다.
 */
object BookCreateAdminClient {
    @JvmStatic
    fun main(args: Array<String>) {
        // region 5.2 AdminClient 생성, 설정, 닫기
        val props = Properties()

        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        // props["request.timeout.ms"] = "120000"

        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        // 4.4: 책 2판이 본 기본 대기는 120초다. 2.5 이후 기본값은 30초다.
        // 이 값은 한 요청의 상한이다. 응답이 없으면 클라이언트가 재시도하거나 그 요청을 실패시킨다.
        props[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = 30_000
        // client.dns.lookup
        // 부트스트랩이 DNS 별칭이면 resolve_canonical_bootstrap_servers_only 가 별칭 안의 브로커 이름을 펼친다.
        // 한 이름이 여러 IP이면 use_all_dns_ips (4.4 기본값)가 그 IP를 차례로 시도한다.
        // localhost는 주소 그대로라 이 키가 필요 없다. "client.dns.lookup"은 살아 있는 코드에 두지 않는다.

        val admin = AdminClient.create(props)
        try {
            // 4.4: 책은 만든 뒤 바로 close한다. create는 소켓을 열지 않는다.
            // 지금은 close 전에 클러스터 ID만 읽어 브로커에 닿았는지 본다.
            val clusterId = admin.describeCluster().clusterId().get()
            logger.info { "admin ready clusterId=$clusterId bootstrap=${KafkaSettings.bootstrapServers()}" }
        } catch (e: Exception) {
            logger.error(e) { "admin bootstrap failed" }
            throw e
        } finally {
            // close(30, TimeUnit.SECONDS)는 3.0에서 삭제됐다. 인자는 Duration이다.
            // 30초 안에 끝나지 않은 요청은 TimeoutException으로 끊기고, 그 다음 자원이 반환된다.
            admin.close(Duration.ofSeconds(30))
        }
        // endregion
    }
}
