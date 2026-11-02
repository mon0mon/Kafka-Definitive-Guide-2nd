plugins {
    alias(libs.plugins.docker.compose)
}

dockerCompose {
    useComposeFiles.set(listOf("docker/compose-single.yml"))
    setProjectName("kdg")
    stopContainers.set(false)
    removeVolumes.set(false)
    waitForTcpPorts.set(true)
    tcpPortsToIgnoreWhenWaiting.set(listOf(8080))
}

val cluster = dockerCompose.createNested("cluster")
cluster.useComposeFiles.set(listOf("docker/compose-cluster.yml"))
cluster.setProjectName("kdg-cluster")
cluster.stopContainers.set(false)
cluster.removeVolumes.set(false)
cluster.waitForTcpPorts.set(true)
