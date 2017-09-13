/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;

import cn.hashdata.bireme.Config.ConnectionConfig;
import cn.hashdata.bireme.provider.DebeziumProvider;
import cn.hashdata.bireme.provider.MaxwellProvider;

/**
 * {@code Bireme} is an incremental synchronization tool. It could sync update in MySQL to GreenPlum
 * / Hashdata database.
 *
 * @author yuze
 *
 */
public class Bireme implements Daemon {
  static final protected Long TIMEOUT_MS = 1000L;
  private static final String DEFAULT_CONFIG_FILE = "etc/config.properties";
  private DaemonContext context;
  private Context cxt;

  private Logger logger = LogManager.getLogger("Bireme." + Bireme.class);

  private ConsoleReporter consoleReporter;
  private JmxReporter jmxReporter;

  protected void parseCommandLine(String[] args)
      throws DaemonInitException, ConfigurationException, BiremeException, InterruptedException {
    Option help = new Option("help", "print this message");
    Option configFile =
        Option.builder("config_file").hasArg().argName("file").desc("config file location").build();

    Options opts = new Options();
    opts.addOption(help);
    opts.addOption(configFile);
    CommandLine cmd = null;
    CommandLineParser parser = new DefaultParser();

    try {
      cmd = parser.parse(opts, args);

      if (cmd.hasOption("help")) {
        throw new ParseException("print help message");
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      StringWriter out = new StringWriter();
      PrintWriter writer = new PrintWriter(out);
      formatter.printHelp(writer, formatter.getWidth(), "Bireme", null, opts,
          formatter.getLeftPadding(), formatter.getDescPadding(), null, true);
      writer.flush();
      String result = out.toString();
      throw new DaemonInitException(result);
    }

    String config = cmd.getOptionValue("config_file", DEFAULT_CONFIG_FILE);

    cxt = new Context(new Config(config));
  }

  protected void getTableInfo() throws BiremeException {
    logger.info("Start getting metadata of target tables from target database.");

    String[] strArray;
    Connection conn = jdbcConn(cxt.conf.target);

    for (String fullname : cxt.tableMap.values()) {
      if (cxt.tablesInfo.containsKey(fullname)) {
        continue;
      }

      strArray = fullname.split("\\.");
      cxt.tablesInfo.put(fullname, new Table(strArray[0], strArray[1], conn));
      logger.trace("Get {}'s metadata.", fullname);
    }

    try {
      conn.close();
    } catch (SQLException ignore) {
    }

    logger.info("Finish getting metadata of target tables from target database.");
  }

  protected void initLoaderConnections() throws BiremeException {
    logger.info("Start establishing connections for loaders.");

    LinkedBlockingQueue<Connection> conns = cxt.loaderConnections;
    HashMap<Connection, HashSet<String>> temporatyTables = cxt.temporaryTables;
    Connection conn = null;

    try {
      for (int i = 0, number = cxt.conf.loader_conn_size; i < number; i++) {
        conn = jdbcConn(cxt.conf.target);
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        stmt.execute("set enable_nestloop = on;");

        try {
          stmt.execute("set gp_autostats_mode = none;");
        } catch (SQLException ignore) {
        }

        conn.setAutoCommit(false);
        conns.add(conn);
        temporatyTables.put(conn, new HashSet<String>());
      }
    } catch (SQLException e) {
      logger.fatal("Could not establish connection to target database.");

      for (Connection closeConn : temporatyTables.keySet()) {
        try {
          closeConn.close();
        } catch (SQLException ignore) {
        }
      }

      throw new BiremeException(e);
    }

    logger.info("Finishing establishing {} connections for loaders.", cxt.conf.loader_conn_size);
  }

  protected void createMaxwellChangeProvider() throws BiremeException {
    for (int i = 0, len = cxt.conf.maxwellConf.size(); i < len; i++) {
      Callable<Long> maxwellProvider = new MaxwellProvider(cxt, cxt.conf.maxwellConf.get(i));
      cxt.cs.submit(maxwellProvider);
    }
  }
  protected void createDebeziumProvider() throws BiremeException {
    for (int i = 0, len = cxt.conf.debeziumConf.size(); i < len; i++) {
      Callable<Long> debeziumProvider = new DebeziumProvider(cxt, cxt.conf.debeziumConf.get(i));
      cxt.cs.submit(debeziumProvider);
    }
  }

  protected void createChangeDispatcher() {
    Callable<Long> dispacher = new Dispatcher(cxt);
    cxt.cs.submit(dispacher);
  }

  protected void createTaskGenerator() {
    Callable<Long> taskGenerator = new TaskGenerator(cxt);
    cxt.cs.submit(taskGenerator);
  }

  protected void createChangeLoaders() {
    HashMap<String, ChangeLoader> loaders = cxt.changeLoaders;
    HashMap<String, String> tableMap = cxt.tableMap;
    ChangeLoader loader;
    String mappedTable;

    for (Entry<String, String> map : tableMap.entrySet()) {
      mappedTable = map.getValue();

      if (!loaders.containsKey(mappedTable)) {
        loader = new ChangeLoader(cxt, mappedTable);
        loaders.put(mappedTable, loader);
        cxt.loadercs.submit(loader);
      }
    }
  }

  /**
   * Start metrics reporter.
   *
   * @throws BiremeException - Wrap and throw Exception which cannot be handled
   */
  protected void startReporter() throws BiremeException {
    switch (cxt.conf.reporter) {
      case "console":
        consoleReporter = ConsoleReporter.forRegistry(cxt.metrics)
                              .convertRatesTo(TimeUnit.SECONDS)
                              .convertDurationsTo(TimeUnit.MILLISECONDS)
                              .build();
        consoleReporter.start(cxt.conf.report_interval, TimeUnit.SECONDS);
        break;
      case "jmx":
        jmxReporter = JmxReporter.forRegistry(cxt.metrics).build();
        jmxReporter.start();
        break;
      default:
        break;
    }
  }

  @Override
  public void init(DaemonContext context) throws DaemonInitException, Exception {
    logger.info("initialize Bireme daemon");
    this.context = context;
    parseCommandLine(context.getArguments());
  }

  @Override
  public void start() throws Exception {
    logger.info("start Bireme daemon");

    getTableInfo();
    initLoaderConnections();

    createChangeLoaders();
    createTaskGenerator();
    createChangeDispatcher();
    createMaxwellChangeProvider();
    createDebeziumProvider();
    startReporter();

    if (context != null) {
      cxt.startWatchDog(context.getController());
    }
  }

  @Override
  public void stop() throws Exception {
    logger.info("stop Bireme daemon");

    if (cxt == null) {
      return;
    }

    cxt.stop = true;
    logger.info("set stop flag to true");

    cxt.threadPool.shutdownNow();
    cxt.loaderThreadPool.shutdownNow();

    cxt.waitForComplete(true);
    logger.info("Bireme exit");
  }

  @Override
  public void destroy() {
    logger.info("destroy Bireme daemon");
  }

  public void entry(String[] args) {
    try {
      parseCommandLine(args);
    } catch (DaemonInitException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    } catch (ConfigurationException | InterruptedException | BiremeException e) {
      e.printStackTrace();
      System.exit(1);
    }

    try {
      start();
      cxt.waitForComplete(false);
    } catch (Exception e) {
      cxt.stop = true;
      e.printStackTrace();

      logger.fatal("Bireme is going to exit since: {}", e.getMessage());

      cxt.threadPool.shutdownNow();
      cxt.loaderThreadPool.shutdownNow();
    }

    try {
      cxt.waitForComplete(true);
    } catch (BiremeException | InterruptedException ignored) {
    }

    logger.info("Bireme exit");
  }

  public static void main(String[] args) {
    Bireme service = new Bireme();
    service.entry(args);
  }

  /**
   * Establish connection to database.
   *
   * @param conf configuration of the aimed database
   * @return the established connection
   * @throws BiremeException - wrap and throw Exception which cannot be handled
   */
  public static Connection jdbcConn(ConnectionConfig conf) throws BiremeException {
    Connection conn = null;

    try {
      conn = DriverManager.getConnection(conf.jdbcUrl, conf.user, conf.passwd);
    } catch (SQLException e) {
      throw new BiremeException(e);
    }

    return conn;
  }
}
