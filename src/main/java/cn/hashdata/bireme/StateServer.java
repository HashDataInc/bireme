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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

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
        gson = new GsonBuilder().setPrettyPrinting().create();
      } else {
        gson = new Gson();
      }

      if (table == null) {
        JsonArray jsonArray = new JsonArray();
        for (String targetTable : cxt.changeLoaders.keySet()) {
          jsonArray.add(getTableState(targetTable));
        }
        result = gson.toJson(jsonArray);
      } else {
        JsonObject jsonObject = getTableState(table);
        result = gson.toJson(jsonObject);
      }

      return result;
    }

    private JsonObject getTableState(String table) {
      SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      LoadState state = cxt.changeLoaders.get(table).state;
      JsonObject jsonFormat = new JsonObject();

      jsonFormat.addProperty("target_table", table);

      if (state == null) {
        jsonFormat.addProperty("State", "Haven't load any task!");
        return jsonFormat;
      }

      JsonArray sources = new JsonArray();

      for (Entry<String, Long> iter : state.produceTime.entrySet()) {
        JsonObject source = new JsonObject();
        String originTable = iter.getKey();
        Long produceTime = iter.getValue();
        Long receiveTime = state.getReceiveTime(originTable);

        String[] split = iter.getKey().split("\\.", 2);

        source.addProperty("source", split[0]);
        source.addProperty("table_name", split[1]);
        source.addProperty("produce_time", sf.format(new Date(produceTime)));
        source.addProperty("receive_time", sf.format(new Date(receiveTime)));

        sources.add(source);
      }

      jsonFormat.add("sources", sources);
      jsonFormat.addProperty("complete_time", sf.format(new Date(state.completeTime)));
      return jsonFormat;
    }
  }
}
