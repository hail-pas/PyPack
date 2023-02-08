package cn.burnish.hbase;

import cn.burnish.config.Config;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HbaseService {
    public Config config;

    public Configuration getConfiguration() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", this.config.hbaseConfig.zookeeperConfig.quorum);
        config.setInt("hbase.zookeeper.property.clientPort", this.config.hbaseConfig.zookeeperConfig.propertyClientPort);
        config.setInt("hbase.regionserver.port", this.config.hbaseConfig.regionServerPort);
        config.setInt("hbase.rpc.timeout", this.config.hbaseConfig.rpcTimeout);
        config.setInt("hbase.client.operation.timeout", this.config.hbaseConfig.clientOperationTimeout);
        config.setInt("hbase.client.scanner.timeout.period", this.config.hbaseConfig.clientScannerTimeoutPeriod);
        return config;
    }

    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(this.getConfiguration());
    }

    public Map<String, String> serialize(@Nullable Set<String> columns, Result result) {
        Map<String, String> resultMap = new HashMap<>();
        if(!result.isEmpty()){
            if (columns != null) {
                for (String i : columns) {
                    String[] parts = i.split(":");
                    resultMap.put(i, new String(result.getValue(parts[0].getBytes(), parts[1].getBytes())));
                }
            } else {
                for (Cell cell : result.rawCells()) {
                    resultMap.put(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
        return resultMap;
    }


    public ArrayList<Map<String, String>>
    scan(
            String tableName,
            @Nullable String rowStart,
            @Nullable String row_Stop,
            @Nullable Filter filter,
            @Nullable Set<String> columns,
            int limit
    ) throws IOException {
        Connection connection = this.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<Map<String, String>> resultArray = new ArrayList<>();
        Scan scan = new Scan(); // TODO
        if (columns != null) {
            for (String i : columns) {
                String[] parts = i.split(":");
                scan.addColumn(parts[0].getBytes(), parts[1].getBytes());
            }
        }
        if (filter != null) scan.setFilter(filter);
        Result[] results = table.getScanner(scan).next(limit);
        for (Result result: results) {
            resultArray.add(this.serialize(columns, result));
        }
        return resultArray;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey rowKey
     * @param columns 列族
     * @return 结果字典
     * @throws IOException
     */
    public Map<String, @Nullable String>
    get(
            String tableName,
            @NotNull String rowKey,
            @Nullable Set<String> columns
    ) throws IOException {
        Connection connection = this.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if (columns != null) {
            for (String i : columns) {
                String[] parts = i.split(":");
                get.addColumn(parts[0].getBytes(), parts[1].getBytes());
            }
        }
        Result result = table.get(get);
        connection.close();
//        result.getValue("family".getBytes(), "qualifier".getBytes());
        return this.serialize(columns, result);
    }

    public void put(String tableName, String rowKey, Map<String, String> values) throws IOException {
        Connection connection = this.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        for (Map.Entry<String, String> entry : values.entrySet()){
            String[] parts = entry.getKey().split(":");
            put.addColumn(parts[0].getBytes(), parts[1].getBytes(), entry.getValue().getBytes());
        }
        table.put(put);
        connection.close();
    }

    public HbaseService(Config config) {
        this.config = config;
    }

    public static void main(String[] args) throws IOException {
        Config conf = new Config();
        conf.loadConfig();
        HbaseService hbaseService = new HbaseService(conf);

        Map<String, @Nullable String> result = hbaseService.get("emp", "1", null);
        System.out.println(result);
//        for (Result result : scanner) {
//            //展示数据
//            for (Cell cell : result.rawCells()) {
//                System.out.println("rowKey=" + Bytes.toString(CellUtil.cloneRow(cell)));
//                System.out.println("family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
//                System.out.println("column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                System.out.println("value=" + Bytes.toString(CellUtil.cloneValue(cell)));
//            }
//        }
    }
}
