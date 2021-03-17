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

        while (!cxt.stop) {
            // start up all normal pipeline
            while (!pipeLineQueue.isEmpty() && !cxt.stop) {
                PipeLine pipeLine = pipeLineQueue.removeFirst();
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
                    // 只有一张表的同步出现中断 result 值才为非空，正常运行情况下都是 null
                    // 为了避免 Schedule 单线程进入死循环的状态，所以需要增加一定的等待时间
                    Future<PipeLine> result = cs.poll(5, TimeUnit.SECONDS);
                    if (result == null) {
                        break;
                    }

                    PipeLine complete = null;
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
