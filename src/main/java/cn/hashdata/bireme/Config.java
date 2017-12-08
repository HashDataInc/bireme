/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.hashdata.bireme.pipeline.SourceConfig;
import cn.hashdata.bireme.pipeline.SourceConfig.SourceType;

/**
 * Configurations about bireme.
 *
 * @author yuze
 *
 */
public class Config {
  private static final String DEFAULT_TABLEMAP_DIR = "etc/";

  private Logger logger = LogManager.getLogger("Bireme." + Config.class);

  private Configuration config;

  public int pipeline_pool_size;

  public int transform_pool_size;
  public int transform_queue_size;

  public int row_cache_size;

  public int merge_pool_size;
  public int merge_interval;
  public int batch_size;

  public int loader_conn_size;
  public int loader_task_queue_size;
  public int loadersCount;

  public String reporter;
  public int report_interval;

  public String state_server_addr;
  public int state_server_port;

  public ConnectionConfig targetDatabase;

  public HashMap<String, SourceConfig> sourceConfig;
  public HashMap<String, String> tableMap;

  public static class ConnectionConfig {
    public String jdbcUrl;
    public String user;
    public String passwd;
  }

  public Config() {
    config = new PropertiesConfiguration();
    basicConfig();
  }

  /**
   * Read config file and store in {@code Config}.
   *
   * @param configFile the config file.
   * @throws ConfigurationException - if an error occurred when loading the configuration
   * @throws BiremeException - wrap and throw Exception which cannot be handled
   */
  public Config(String configFile) throws ConfigurationException, BiremeException {
    Configurations configs = new Configurations();
    config = configs.properties(new File(configFile));

    basicConfig();
    connectionConfig("target");
    dataSourceConfig();

    logConfig();
  }

  protected void basicConfig() {
    pipeline_pool_size = config.getInt("pipeline.thread_pool.size", 5);

    transform_pool_size = config.getInt("transform.thread_pool.size", 10);
    transform_queue_size = transform_pool_size;

    merge_pool_size = config.getInt("merge.thread_pool.size", 10);
    merge_interval = config.getInt("merge.interval", 10000);
    batch_size = config.getInt("merge.batch.size", 50000);
    row_cache_size = batch_size * 2;

    loader_conn_size = config.getInt("loader.conn_pool.size", 10);
    loader_task_queue_size = config.getInt("loader.task_queue.size", 2);

    reporter = config.getString("metrics.reporter", "console");
    report_interval = config.getInt("metrics.reporter.console.interval", 15);

    state_server_addr = config.getString("state.server.addr", "0.0.0.0");
    state_server_port = config.getInt("state.server.port", 8080);

    sourceConfig = new HashMap<String, SourceConfig>();
    tableMap = new HashMap<String, String>();
  }

  /**
   * Get the connection configuration to database.
   *
   * @param prefix "target" database
   * @throws BiremeException when url of database is null
   */
  protected void connectionConfig(String prefix) throws BiremeException {
    Configuration subConfig = new SubsetConfiguration(config, "target", ".");
    targetDatabase = new ConnectionConfig();

    targetDatabase.jdbcUrl = subConfig.getString("url");
    targetDatabase.user = subConfig.getString("user");
    targetDatabase.passwd = subConfig.getString("passwd");

    if (targetDatabase.jdbcUrl == null) {
      String message = "Please designate url for target Database.";
      throw new BiremeException(message);
    }
  }

  protected void dataSourceConfig() throws BiremeException, ConfigurationException {
    String[] sources = config.getString("data_source").replaceAll("[ \f\n\r\t]", "").split(",");
    if (sources == null || sources.length == 0) {
      String message = "Please designate at least one data source.";
      logger.fatal(message);
      throw new BiremeException(message);
    }
    for (int i = 0; i < sources.length; i++) {
      sourceConfig.put(sources[i], new SourceConfig(sources[i]));
    }

    fetchSourceAndTableMap();
  }

  /**
   * Get the {@code Provider} configuration.
   *
   * @throws BiremeException miss some required configuration
   * @throws ConfigurationException if an error occurred when loading the configuration
   */
  protected void fetchSourceAndTableMap() throws BiremeException, ConfigurationException {
    loadersCount = 0;

    for (SourceConfig conf : sourceConfig.values()) {
      String type = config.getString(conf.name + ".type");
      if (type == null) {
        String message = "Please designate the data source type of " + conf.name;
        logger.fatal(message);
        throw new BiremeException(message);
      }

      switch (type) {
        case "maxwell":
          fetchMaxwellConfig(conf);
          break;

        case "debezium":
          fetchDebeziumConfig(conf);
          break;

        default:
          String message = "Unrecognized type for data source " + conf.name;
          logger.fatal(message);
          throw new BiremeException(message);
      }

      conf.tableMap = fetchTableMap(conf.name);
    }

    if (loader_conn_size > loadersCount) {
      loader_conn_size = loadersCount;
    }
  }

  /**
   * Get DebeziumSource configuration.
   *
   * @param debeziumConf An empty {@code SourceConfig}
   * @throws BiremeException miss some required configuration
   */
  protected void fetchDebeziumConfig(SourceConfig debeziumConf) throws BiremeException {
    Configuration subConfig = new SubsetConfiguration(config, debeziumConf.name, ".");

    String prefix = subConfig.getString("namespace");
    if (prefix == null) {
      String messages = "Please designate your namespace.";
      logger.fatal(messages);
      throw new BiremeException(messages);
    }

    debeziumConf.type = SourceType.DEBEZIUM;
    debeziumConf.server = subConfig.getString("kafka.server");
    debeziumConf.topic = prefix;
    debeziumConf.groupID = subConfig.getString("kafka.groupid", "bireme");

    if (debeziumConf.server == null) {
      String message = "Please designate server for " + debeziumConf.name + ".";
      logger.fatal(message);
      throw new BiremeException(message);
    }
  }

  /**
   * Get MaxwellConfig configuration.
   *
   * @param maxwellConf an empty {@code SourceConfig}
   * @throws BiremeException miss some required configuration
   */
  protected void fetchMaxwellConfig(SourceConfig maxwellConf) throws BiremeException {
    String prefix = maxwellConf.name;
    Configuration subConfig = new SubsetConfiguration(config, prefix, ".");

    maxwellConf.type = SourceType.MAXWELL;
    maxwellConf.server = subConfig.getString("kafka.server");
    maxwellConf.topic = subConfig.getString("kafka.topic");
    maxwellConf.groupID = subConfig.getString("kafka.groupid", "bireme");

    if (maxwellConf.server == null) {
      String message = "Please designate server for " + prefix + ".";
      throw new BiremeException(message);
    }

    if (maxwellConf.topic == null) {
      String message = "Please designate topic for " + prefix + ".";
      throw new BiremeException(message);
    }
  }

  private HashMap<String, String> fetchTableMap(String dataSource)
      throws ConfigurationException, BiremeException {
    Configurations configs = new Configurations();
    Configuration tableConfig = null;

    tableConfig = configs.properties(new File(DEFAULT_TABLEMAP_DIR + dataSource + ".properties"));

    String originTable, mappedTable;
    HashMap<String, String> localTableMap = new HashMap<String, String>();
    Iterator<String> tables = tableConfig.getKeys();

    while (tables.hasNext()) {
      originTable = tables.next();
      mappedTable = tableConfig.getString(originTable);

      if (originTable.split("\\.").length != 2 || mappedTable.split("\\.").length != 2) {
        String message = "Wrong format: " + originTable + ", " + mappedTable;
        logger.fatal(message);
        throw new BiremeException(message);
      }

      localTableMap.put(dataSource + "." + originTable, mappedTable);

      if (!tableMap.values().contains(mappedTable)) {
        loadersCount++;
      }
      tableMap.put(dataSource + "." + originTable, mappedTable);
    }

    return localTableMap;
  }

  /**
   * Print log about bireme configuration.
   */
  public void logConfig() {
    String config = "Configures: "
        + "\n\tpipeline thread pool size = " + pipeline_pool_size
        + "\n\ttransform thread pool size = " + transform_pool_size + "\n\ttransform queue size = "
        + transform_queue_size + "\n\trow cache size = " + row_cache_size
        + "\n\tmerge thread pool size = " + merge_pool_size
        + "\n\tmerge interval = " + merge_interval + "\n\tbatch size = " + batch_size
        + "\n\tloader connection size = " + loader_conn_size + "\n\tloader task queue size = "
        + loader_task_queue_size + "\n\tloaders count = " + loadersCount
        + "\n\treporter = " + reporter + "\n\treport interval = " + report_interval
        + "\n\tstate server addr = " + state_server_addr + "\n\tstate server port = "
        + state_server_port + "\n\ttarget database url = " + targetDatabase.jdbcUrl;

    logger.info(config);

    StringBuilder sb = new StringBuilder();
    sb.append("Data Source: \n");
    for (SourceConfig conf : sourceConfig.values()) {
      sb.append("\tType: " + conf.type.name() + " Name: " + conf.name + "\n");
    }

    logger.info(sb.toString());
  }
}
