package xyz.mon0mon.ch05.s06_cluster_metadata

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.DescribeClusterOptions
import xyz.mon0mon.common.KafkaSettings

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.6 클러스터 메타데이터
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `AdminClient.create(Properties)`와 `describeCluster()` ([BookClusterMetadata]).
 * - 2.3  `includeAuthorizedOperations`. 인가자가 없으면 결과는 null이다.
 * - 2.8  DescribeCluster RPC (KIP-700).
 * - 3.7  `bootstrap.controllers` (KIP-919). 컨트롤러 노드를 브로커와 따로 조회한다.
 * - 4.0  `includeFencedBrokers(true)`면 펜스된 브로커도 `nodes()`에 들어오고 `Node.isFenced()`가 참이다.
 * - 4.4  `Admin.create(Map)`, `client.id`, `DescribeClusterOptions`, `use`.
 *
 * Replaces: [BookClusterMetadata]
 * Still valid: `clusterId()`, `nodes()`, `controller()`, `Node.hasRack()`, `Node.rack()`
 *
 * 흐름:
 * 1. `Map`으로 `Admin`을 연다. 컨트롤러 리스너 포트는 이 Compose에서 호스트에 열려 있지 않아 브로커 부트스트랩을 쓴다.
 * 2. 10초 제한과 `includeFencedBrokers(true)`로 클러스터를 조회한다.
 * 3. 클러스터 ID, 브로커의 id/host/port/rack/fenced, 활성 컨트롤러를 로그로 남긴다.
 * 4. `use`가 `close()`를 호출한다.
 */
object CurrentClusterMetadata {
    private const val CLIENT_ID = "ch05-s06-current"

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to CLIENT_ID,
        )
        // bootstrap.controllers 는 컨트롤러 리스너(이 실습은 kafka:9093)로 Admin을 붙인다.
        // 호스트에는 9092만 열려 있다. 브로커가 알려 주는 활성 컨트롤러를 읽는다.

        // region 5.6 클러스터 메타데이터
        Admin.create(configs).use { admin ->
            // 기본 describeCluster는 살아 있는 브로커만 준다. 펜스된 브로커까지 보려면 이 옵션이 필요하다.
            val described = admin.describeCluster(
                DescribeClusterOptions()
                    .timeoutMs(10_000)
                    .includeFencedBrokers(true),
            )

            val clusterId = described.clusterId().get()
            val nodes = described.nodes().get().sortedBy { it.id() }
            // KRaft 합쳐진 노드에서는 이 브로커가 곧 활성 컨트롤러다. 분리된 쿼럼이면 브로커 목록 밖의 ID일 수 있다.
            val controller = described.controller().get()

            logger.info {
                "clusterId=$clusterId clientId=$CLIENT_ID brokers=${nodes.size} controllerId=${controller?.id()}"
            }
            nodes.forEach { node ->
                // hasRack()이 거짓이면 rack()은 null이다. 문자열 "null" 대신 없음을 적는다.
                val rack = if (node.hasRack()) node.rack() else "none"
                logger.info {
                    "broker id=${node.id()} host=${node.host()} port=${node.port()} rack=$rack fenced=${node.isFenced()}"
                }
            }
            if (controller == null) {
                logger.info { "controller is empty" }
            } else {
                logger.info {
                    "controller id=${controller.id()} host=${controller.host()} port=${controller.port()}"
                }
            }
        }
        // endregion
    }
}
