package cn.hashdata.bireme;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.hashdata.bireme.pipeline.PipeLine;

/**
 * {@code Scheduler} collects all {@code PipeLine}. Successively and constantly drive the
 * {@code PipeLine}s to work.
 *
 * @author yuze
 *
 */
public class Scheduler implements Callable<Long> {
  public Logger logger = LogManager.getLogger("Bireme.Scheduler");

  public Context cxt;
  public CompletionService<PipeLine> cs;
  public LinkedList<PipeLine> pipeLineQueue;
  public int workingPipeLine;

  public Scheduler(Context cxt) {
    this.cxt = cxt;
    this.cs = new ExecutorCompletionService<PipeLine>(cxt.pipeLinePool);
    this.pipeLineQueue = new LinkedList<PipeLine>();

    Iterator<PipeLine> iter = cxt.pipeLines.iterator();
    while (iter.hasNext()) {
      pipeLineQueue.add(iter.next());
    }
    workingPipeLine = 0;
  }

  @Override
  public Long call() throws BiremeException, InterruptedException {
    logger.info("Scheduler start working.");

    PipeLine pipeLine = null;

    while (!cxt.stop) {
      // start up all normal pipeline
      while (!pipeLineQueue.isEmpty() && !cxt.stop) {
        pipeLine = pipeLineQueue.removeFirst();
        switch (pipeLine.state) {
          case NORMAL:
            cs.submit(pipeLine);
            workingPipeLine++;
            break;
          case ERROR:
          default:
        }
      }

      // get result of all completed pipeline
      if (workingPipeLine != 0) {
        while (!cxt.stop) {
          Future<PipeLine> result = cs.poll();
          PipeLine complete = null;

          if (result == null) {
            break;
          }

          try {
            complete = result.get();
          } catch (ExecutionException e) {
            logger.warn("Pipeline throw out exception. Message {}", e.getMessage());

            throw new BiremeException("Schedule Exception", e.getCause());
          }

          pipeLineQueue.add(complete);
          workingPipeLine--;
        }
      } else {
        logger.info("All pipeline stop.");
        break;
      }
    }

    return 0L;
  }
}
