package cn.hashdata.bireme;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import cn.hashdata.bireme.provider.PipeLine;
import cn.hashdata.bireme.provider.PipeLine.PipeLineState;

public class Scheduler implements Callable<Long> {
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
  public Long call() throws BiremeException {
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
            // TODO print error message and stack
            pipeLine.state = PipeLineState.STOP;
            break;
          case STOP:
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
          } catch (ExecutionException | InterruptedException e) {
            throw new BiremeException("PipeLine shouldn't throw out exception.", e);
          }
          
          pipeLineQueue.add(complete);
          workingPipeLine--;
        }
      } else {
        // TODO print log, all pipeline stop
        break;
      }
    }

    return 0L;
  }
}
