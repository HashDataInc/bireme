package cn.hashdata.bireme;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cn.hashdata.bireme.LoadState.PlainState;

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

    /**
     * Create a new StateHandler
     */
    public StateHandler() {
      this.table = null;
    }

    /**
     * Create a new StateHandler
     *
     * @param table set the target table for this handler
     */
    public StateHandler(String table) {
      this.table = table;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      response.setContentType("text/html; charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);

      PrintWriter out = response.getWriter();
      String format = request.getParameter("pretty");
      out.println(fetchState(format));

      baseRequest.setHandled(true);
    }

    /**
     * Get the State.
     *
     * @param format if not null, format the output string in a pretty style
     * @return the state
     */
    public String fetchState(String format) {
      String result;
      Gson gson = null;

      if (format != null) {
        gson = new GsonBuilder()
                   .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                   .setPrettyPrinting()
                   .create();
      } else {
        gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
      }

      if (table == null) {
        StringBuilder sb = new StringBuilder();

        for (String targetTable : cxt.changeLoaders.keySet()) {
          sb.append(gson.toJson(getTableState(targetTable), PlainState.class));
        }

        result = sb.toString();
      } else {
        result = gson.toJson(getTableState(table), PlainState.class);
      }

      return result;
    }

    private PlainState getTableState(String table) {
      ChangeLoader loader = cxt.changeLoaders.get(table);
      LoadState state = loader.getLoadState();

      return state.getPlainState(table);
    }
  }
}
