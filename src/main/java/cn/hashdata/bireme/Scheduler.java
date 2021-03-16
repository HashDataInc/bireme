package cn.hashdata.bireme;

import cn.hashdata.bireme.pipeline.PipeLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.*;

/**
 * {@code Scheduler} collects all {@code PipeLine}. Successively and constantly drive the
 * {@code PipeLine}s to work.
 *
 * @author yuze
 */
public class Scheduler implements Callable<Long> {
    public Logger logger = LogManager.getLogger(Scheduler.class);

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

                    // TODO:如果为空，是否要等待一下在执行？否则此处会造成死循环？
                    // 还是说 cs.poll() 可以实现阻塞的效果
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
