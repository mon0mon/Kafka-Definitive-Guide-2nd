package xyz.mon0mon.ch05.s06_cluster_metadata

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.6 클러스터 메타데이터
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `describeCluster()`가 브로커 `Node` 목록과 컨트롤러를 `KafkaFuture`로 준다.
 * - 2.3  `authorizedOperations()` (KIP-430). 이 절의 출력에는 넣지 않는다.
 * - 2.8  DescribeCluster RPC (KIP-700). 그 전에는 Metadata 요청으로 같은 필드를 채웠다.
 * - 3.7  `bootstrap.controllers` (KIP-919). 컨트롤러 리스너에 직접 붙을 수 있다.
 * - 4.0  `includeFencedBrokers`, `Node.isFenced()` (KIP-1073).
 * - 4.4  `Admin.create`와 `use`가 권장 → [CurrentClusterMetadata].
 *
 * Still valid: `AdminClient.create(Properties)`, `describeCluster()`, `clusterId()`, `nodes()`,
 *              `controller()`, `Node.rack()`
 *
 * 흐름:
 * 1. 5.2와 같이 `Properties`와 `AdminClient.create`로 클라이언트를 만든다. 이 호출은 브로커에 요청을 보내지 않는다.
 * 2. `describeCluster()`로 클러스터 ID, 브로커 목록, 랙, 현재 컨트롤러를 요청한다.
 * 3. 각 결과는 `KafkaFuture`다. `get()`이 브로커 응답을 기다린다.
 * 4. `close()`로 클라이언트를 닫는다.
 */
object BookClusterMetadata {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = Properties()

        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()

        val admin = AdminClient.create(props)
        try {
            // region 5.6 클러스터 메타데이터
            // 반환값은 결과 묶음이다. 네트워크 대기는 아래 get()에서 일어난다.
            val cluster = admin.describeCluster()

            // 클러스터 ID는 브로커가 처음 기동할 때 정한 값이다. 재기동해도 유지된다.
            logger.info { "Connected to cluster ${cluster.clusterId().get()}" }
            logger.info { "The brokers in the cluster are:" }
            // nodes()는 클라이언트가 본 브로커다. 기본 응답에는 펜스된 브로커가 없다.
            cluster.nodes().get().forEach { node ->
                logger.info { "    * $node" }
                // 랙이 없으면 null이다. 이 실습 브로커는 broker.rack을 넣지 않았다.
                logger.info { "      Rack: ${node.rack()}" }
            }

            // 4.4: 책은 주키퍼 시절의 단일 활성 컨트롤러를 읽는다. API는 그대로다.
            // KRaft에서는 메타데이터 쿼럼의 활성 컨트롤러다. 합쳐진 단일 노드는 브로커이면서 컨트롤러다.
            val controller = cluster.controller().get()
            logger.info { "The controller is: $controller" }
            // endregion
        } catch (e: Exception) {
            logger.error(e) { "describe cluster failed" }
            throw e
        } finally {
            admin.close()
        }
    }
}
