package cn.burnish.config;

import cn.burnish.config.hbase.HbaseConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    public static final Logger logger = LoggerFactory.getLogger(Config.class);
    public HbaseConfig hbaseConfig;

    public Config() {
    }

    public static String readJsonFile(String fileName) {
        String jsonStr;

        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            StringBuilder sb = new StringBuilder();

            int ch;
            while((ch = reader.read()) != -1) {
                sb.append((char)ch);
            }

            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException var7) {
            throw new RuntimeException(var7);
        }
    }

    public void LoadHbaseConfig(Map<String, Object> config) {
        this.hbaseConfig = new HbaseConfig(config);
    }

    public void loadConfig() {
        String environment = System.getProperty("releaseEnvironment");
        logger.info("Environment is: " + environment + "\n");
        Path path = Paths.get(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
        path = Paths.get(path.getParent().toString(), "config", environment + ".json");
        logger.info("Configuration Path is: " + path);
        String content = readJsonFile(path.toString());
        JSONObject json = JSON.parseObject(content);
        JSONArray enabledModules = json.getJSONArray("enabledModules");

        System.out.println("=============================");
        System.out.println("Enabled Modules: ");
        for(Object moduleIter: enabledModules) {
            String moduleName = moduleIter.toString();
            System.out.println("\t" + moduleName);
            JSONObject moduleConfig = json.getJSONObject("moduleConfigs").getJSONObject(moduleName);
            switch (moduleName) {
                case "hbase" -> this.LoadHbaseConfig(moduleConfig);
                case "redis" -> System.out.println("redis");
            }
        }
        System.out.println("=============================");

    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.loadConfig();
        System.out.println(conf.hbaseConfig.zookeeperConfig.quorum);
        System.out.println(conf.hbaseConfig.zookeeperConfig.propertyClientPort);
    }
}
