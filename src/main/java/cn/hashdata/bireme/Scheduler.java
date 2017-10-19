package cn.hashdata.bireme;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import cn.hashdata.bireme.provider.Provider;

public class Scheduler implements Callable<Long> {
  Context cxt;
  LinkedList<Provider> pipeLineQueue;

  public Scheduler(Context cxt) {
    this.cxt = cxt;
    this.pipeLineQueue = new LinkedList<Provider>();

    Iterator<Provider> iter = cxt.pipeLines.iterator();

    while (iter.hasNext()) {
      pipeLineQueue.add(iter.next());
    }
  }

  @Override
  public Long call() throws InterruptedException {
    Provider pipeLine = null;

    while (!cxt.stop) {
      pipeLine = pipeLineQueue.removeFirst();
      try {
        pipeLine.execute();
      } catch (BiremeException e) {
        // TODO A pipeLine failed.
      }
      pipeLineQueue.add(pipeLine);
    }

    return 0L;
  }
}
