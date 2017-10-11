package cn.hashdata.bireme;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.Request;

/**
 * {@code StateServer} is a simple http server to get the load status via http protocol.
 *
 * @author yuze
 *
 */
public class StateServer {
  public Context cxt;
  public Server server;

  /**
   * Create a StateServer
   *
   * @param cxt the Bireme Context
   * @param port the binded port of the server
   */
  public StateServer(Context cxt, int port) {
    this.cxt = cxt;
    this.server = new Server(port);
  }

  /**
   * Start the http server.
   */
  public void start() {
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    ContextHandler context = new ContextHandler("/");
    context.setHandler(new StateHandler());
    contexts.addHandler(context);

    for (String table : cxt.changeLoaders.keySet()) {
      context = new ContextHandler("/" + table);
      context.setHandler(new StateHandler(table));
      contexts.addHandler(context);
    }

    server.setHandler(contexts);

    try {
      server.start();
    } catch (Exception e) {
    }
  }

  /**
   * Stop the http server.
   */
  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
    }
  }

  /**
   * The Handler for StateServer.
   *
   * @author yuze
   *
   */
  class StateHandler extends AbstractHandler {
    final String table;
    StringBuilder outPut;

    public StateHandler() {
      this.table = null;
      this.outPut = new StringBuilder();
    }

    public StateHandler(String table) {
      this.table = table;
      this.outPut = new StringBuilder();
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      response.setContentType("text/html; charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);

      PrintWriter out = response.getWriter();

      out.println(fetchState());

      baseRequest.setHandled(true);
    }

    public String fetchState() {
      outPut.setLength(0);
      if (table == null) {
        for (String targetTable : cxt.changeLoaders.keySet()) {
          getTableState(targetTable);
          outPut.append("\n\n");
        }
        return outPut.toString();
      } else {
        getTableState(table);
        return outPut.toString();
      }
    }

    private void getTableState(String table) {
      outPut.append("Table: " + table + "\n");
      SimpleDateFormat sf = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
      LoadState state = cxt.changeLoaders.get(table).state;

      if (state == null) {
        outPut.append("haven't load any task.");
        return;
      }

      for (Entry<String, Long> iter : state.produceTime.entrySet()) {
        outPut.append("Source Table: " + iter.getKey() + " Produce Time: "
            + sf.format(new Date(iter.getValue())) + "\n");
      }

      for (Entry<String, Long> iter : state.receiveTime.entrySet()) {
        outPut.append("Source Table: " + iter.getKey() + " Produce Time: "
            + sf.format(new Date(iter.getValue())) + "\n");
      }
      outPut.append("Complete Time: " + sf.format(new Date(state.completeTime)) + "\n");
    }
  }
}
