package cn.burnish.config.hbase;

import java.util.Map;

public class HbaseConfig {
    public ZookeeperConfig zookeeperConfig;
    public HbaseConfig(Map<String, Object> hbaseConfig) {
        this.zookeeperConfig = new ZookeeperConfig(hbaseConfig.get("hbase.zookeeper.quorum"), hbaseConfig.get("hbase.zookeeper.property.clientPort"));
    }
}
