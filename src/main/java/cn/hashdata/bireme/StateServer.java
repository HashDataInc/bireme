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
import cn.hashdata.bireme.pipeline.PipeLine.PipeLineState;
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
      response.setContentType("application/json; charset=utf-8");
      if (target.compareTo("/") != 0 && this.source == null) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);

        Gson gson = new Gson();
        Message message = new Message("the datasource is not found");
        String format = gson.toJson(message);
        PrintWriter out = response.getWriter();
        out.println(format);
      } else {
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter out = response.getWriter();
        String format = request.getParameter("pretty");
        out.println(fetchState(format));
      }

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
      Source e = new Source(conf.name, conf.type.toString());

      for (PipeLine p : conf.pipeLines) {
        String name = p.myName.split("-", 3)[2];
        PipeLineStat stat = p.stat;
        Date latest = new Date(stat.newestCompleted);
        long delay = stat.delay;
        String pipelinestate = "NORMAL";
        if (p.state == PipeLineState.ERROR) {
          pipelinestate = "ERROR";
        }

        e.pipelines.add(new Stat(name, latest, delay, pipelinestate));
      }

      return e;
    }
  }

  class Stat {
    String name;
    Date latest;
    double delay;
    String state;

    public Stat(String name, Date latest, long delay, String state) {
      this.name = name;
      this.latest = latest;
      this.delay = delay / 1000.0;
      this.state = state;
    }
  }
  class Source {
    String source_name;
    String type;
    ArrayList<Stat> pipelines;

    public Source(String source_name, String type) {
      this.source_name = source_name;
      this.type = type;
      pipelines = new ArrayList<Stat>();
    }
  }
  class Message {
    String errorMessage;

    public Message(String errorMessage) {
      this.errorMessage = errorMessage;
    }
  }
}
