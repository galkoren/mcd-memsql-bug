package com.kenshoo.mcdownload.test;

import com.google.common.collect.Iterables;
import com.kenshoo.mcdownload.data.model.AdGroupLevelId;
import com.kenshoo.mcdownload.data.model.ChangeDetectorRequest;
import com.kenshoo.mcdownload.data.model.ChannelType;
import com.kenshoo.mcdownload.services.ChangeDetector;
import com.kenshoo.mcdownload.services.ProducerBlockingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kenshoo.mcdownload.test.DataGenerator.generateFakeItems;
import static org.jooq.lambda.Seq.seq;

public class MemSqlTest {

    // -------------- test params ------------ //
    private final int JOB_SIZE = 300_000;
    private final int NUM_OF_THREADS = 10;
    // -------------- test params ------------ //

    // boilterplate
    public static final int ACCOUNT_ID = 450;
    private final static Logger logger = LoggerFactory.getLogger(MemSqlTest.class);
    private final AtomicInteger nextJobIdSuffix = new AtomicInteger(1);
    private final ChangeDetector changeDetector = new ChangeDetector();

    private Executor consumerPool;
    private Thread producerThread;

    public static void main(String[] args) throws Exception {
        new MemSqlTest().startTest();
    }

    private void startTest() {
        consumerPool = new ProducerBlockingExecutor(NUM_OF_THREADS, NUM_OF_THREADS);

        producerThread = new Thread(() -> {
            while (true) consumerPool.execute(this::runSingleJob);
        });

        producerThread.start();
    }

    private void runSingleJob() {
        final String jobId = "job_" + nextJobIdSuffix.incrementAndGet();

        logger.info("----------------------------------------------------------------");
        logger.info("----- loading " + JOB_SIZE + " items");

        List<ChangeDetectorRequest<AdGroupLevelId>> items = generateFakeItems(JOB_SIZE).toList();

        int count = seq(Iterables.partition(items, 80000)).map(bulk -> changeDetector.detectChanges(
                bulk,
                ACCOUNT_ID,
                ChannelType.GOOGLE,
                "client",
                jobId
        )).sumInt(x -> x);

        logger.info("----- count " + count + " out of " + JOB_SIZE);

        if (count != JOB_SIZE) {
            logger.warn("--------------------- !!!!!!!!! COUNT MISMATCH !!!!!!!!!!!! ---------------------");
        }
        changeDetector.getToDelete(450, "client", jobId, getCampaignIds(items)).toList();
        changeDetector.cleanJob(jobId, JOB_SIZE);

    }

    private static List<String> getCampaignIds(List<ChangeDetectorRequest<AdGroupLevelId>> items) {
        return seq(items).map(i -> Long.toString(i.getId().getCampId())).distinct().toList();
    }



}
