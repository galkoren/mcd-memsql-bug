package com.kenshoo.mcdownload.services;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class ProducerBlockingExecutor implements Executor  {

    private final Semaphore semaphore;
    private final AtomicReference<ExecutorService> innerExecutor;
    private int size;

    public ProducerBlockingExecutor(int threads, int semaphoreSize) {
        innerExecutor = new AtomicReference<>(createThreadPool(threads));
        semaphore = new Semaphore(semaphoreSize, true);
        this.size = semaphoreSize;
    }

    public void resize(int newThreads, int newSize) {
        ExecutorService oldExecutor = innerExecutor.get();
        innerExecutor.set(createThreadPool(newThreads));
        oldExecutor.shutdown();

        if (newSize > size) {
            semaphore.release(newSize - size);
        } else if (newSize < size) {
            new Thread(() -> acquirePermit(semaphore, size - newSize)).start();
        }
        this.size = newSize;
    }

    private ThreadPoolExecutor createThreadPool(int newThreads) {
        return new ThreadPoolExecutor(newThreads, newThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    private void acquirePermit(Semaphore semaphore, int i) {
        while (i-- > 0) {
            try {
                semaphore.acquire();
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void execute(Runnable command) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        innerExecutor.get().execute(() -> {
            try {
                command.run();
            } finally {
                semaphore.release();
            }
        });
    }
}
