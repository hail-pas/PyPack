package cn.burnish.config.hbase;

public class ZookeeperConfig {
    public String quorum;
    public int propertyClientPort;

    ZookeeperConfig(Object quorum, Object propertyClientPort) {
        this.quorum = (String)quorum;
        this.propertyClientPort = (int)propertyClientPort;
    }
}
