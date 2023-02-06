package cn.burnish.hbase;

import cn.burnish.config.Config;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.jetbrains.annotations.NotNull;

public class HbaseService {
    public Config config;

    public Configuration getConfiguration() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", this.config.hbaseConfig.zookeeperConfig.quorum);
        config.set("hbase.zookeeper.property.clientPort", this.config.hbaseConfig.zookeeperConfig.propertyClientPort);
        return config;
    }

    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(this.getConfiguration());
    }

    public Result getRow(String tableName, @NotNull String rowKey) throws IOException {
        Connection connection = this.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        connection.close();
        return result;
    }

    public HbaseService(Config config) {
        this.config = config;
    }

    public static void main(String[] args) throws IOException {
        Config conf = new Config();
        conf.loadConfig();
        HbaseService hbaseService = new HbaseService(conf);
        Result result = hbaseService.getRow("dev_obd_info", "AC202211143LAANY7__2022-11-18 10:59:35");
        System.out.println(result);
    }
}
