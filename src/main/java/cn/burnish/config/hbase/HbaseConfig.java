package cn.burnish.config.hbase;

import java.util.Map;

public class HbaseConfig {
    public ZookeeperConfig zookeeperConfig;
    public int regionServerPort;
    public int rpcTimeout;
    public int clientOperationTimeout;
    public int clientScannerTimeoutPeriod;

    public HbaseConfig(Map<String, Object> hbaseConfig) {
        this.zookeeperConfig = new ZookeeperConfig(hbaseConfig.get("hbase.zookeeper.quorum"), hbaseConfig.get("hbase.zookeeper.property.clientPort"));
        this.regionServerPort = (int)hbaseConfig.get("hbase.regionserver.port");
        this.rpcTimeout = (int)hbaseConfig.get("hbase.rpc.timeout");
        this.clientOperationTimeout = (int) hbaseConfig.get("hbase.client.operation.timeout");
        this.clientScannerTimeoutPeriod = (int) hbaseConfig.get("hbase.client.scanner.timeout.period");
    }
}
