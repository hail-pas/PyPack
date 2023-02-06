package cn.burnish;

import cn.burnish.config.Config;
import cn.burnish.hbase.HbaseService;
import py4j.GatewayServer;

public class Main {
    public final HbaseService hbaseService;

    public Main() {
        Config conf = new Config();
        conf.loadConfig();
        this.hbaseService = new HbaseService(conf);
    }

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new Main());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
