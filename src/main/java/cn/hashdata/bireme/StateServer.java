package cn.hashdata.bireme;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cn.hashdata.bireme.pipeline.PipeLine;
import cn.hashdata.bireme.pipeline.SourceConfig;

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
   * @param addr the binded IP address for the server
   * @throws BiremeException Unknown Host
   */
  public StateServer(Context cxt, String addr, int port) throws BiremeException {
    this.cxt = cxt;
    try {
      this.server = new Server(new InetSocketAddress(InetAddress.getByName(addr), port));
    } catch (UnknownHostException e) {
      String message = "Unknown Host";
      throw new BiremeException(message, e);
    }
  }

  /**
   * Start the http server.
   */
  public void start() {
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    ContextHandler context = new ContextHandler("/");
    context.setHandler(new StateHandler());
    contexts.addHandler(context);

    for (SourceConfig conf : cxt.conf.sourceConfig.values()) {
      context = new ContextHandler("/" + conf.name);
      context.setHandler(new StateHandler(conf));
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
    final SourceConfig source;

    /**
     * Create a new StateHandler
     */
    public StateHandler() {
      this.source = null;
    }

    /**
     * Create a new StateHandler
     *
     * @param table set the target table for this handler
     */
    public StateHandler(SourceConfig conf) {
      this.source = conf;
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

      if (source == null) {
        StringBuilder sb = new StringBuilder();

        for (SourceConfig conf : cxt.conf.sourceConfig.values()) {
          sb.append(gson.toJson(getSourceState(conf.name), Source.class));
        }

        result = sb.toString();
      } else {
        result = gson.toJson(getSourceState(source.name), Source.class);
      }

      return result;
    }

    private Source getSourceState(String sourceName) {
      SourceConfig conf = cxt.conf.sourceConfig.get(sourceName);
      Source e = new Source(conf.name);

      for (PipeLine p : conf.pipeLines) {
        String name = p.myName;
        PipeLineStat stat = p.stat;
        Date newest_record = new Date(stat.newestCompleted);
        long delay = stat.delay;

        e.pipelines.add(new Stat(name, newest_record, delay));
      }

      return e;
    }
  }

  class Stat {
    String name;
    Date newest_record;
    double delay;

    public Stat(String name, Date newest_record, long delay) {
      this.name = name;
      this.newest_record = newest_record;
      this.delay = delay / 1000.0;
    }
  }
  class Source {
    String source_name;
    ArrayList<Stat> pipelines;

    public Source(String source_name) {
      this.source_name = source_name;
      pipelines = new ArrayList<Stat>();
    }
  }
}
