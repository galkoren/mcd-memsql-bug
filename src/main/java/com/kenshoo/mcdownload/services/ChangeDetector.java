package com.kenshoo.mcdownload.services;

import com.kenshoo.mcdownload.data.model.AdGroupLevelId;
import com.kenshoo.mcdownload.data.model.ChangeDetectorRequest;
import com.kenshoo.mcdownload.data.model.ChannelType;
import com.kenshoo.mcdownload.data.model.DetectedChange;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kenshoo.mcdownload.data.model.ChangeType.DELETED;
import static org.jooq.lambda.Seq.seq;


public class ChangeDetector {

    private static final Logger logger = LoggerFactory.getLogger(ChangeDetector.class);
    private AdGroupLevelDigestDao digestDao = new AdGroupLevelDigestDao();

    static AtomicInteger bulkId = new AtomicInteger(1);

    public int detectChanges(
            List<ChangeDetectorRequest<AdGroupLevelId>> entities,
            long customerId,
            ChannelType channelType,
            String clientName,
            String jobId) {

        int bulk = bulkId.incrementAndGet();
        digestDao.loadAccountRecordsToSideTableFromFile(entities, customerId, jobId, bulk, clientName, channelType);
        Set<AdGroupLevelId> changedDigests = notSameDigestInDB(customerId, clientName, jobId, bulk);
        return changedDigests.size();
    }

    public void cleanJob(String jobId, int loadCounter) {
        int fetched = digestDao.countSideTable(jobId);
        int deleted = digestDao.deleteAccountRecordsFromSideTable(jobId);
        if (deleted != fetched || deleted != loadCounter) {
            logger.warn("Deleted count from side table is not equal to fetched or to loaded count. Jobid: {}, loaded: {}, fetched: {}, deleted: {}", jobId , loadCounter, fetched, deleted);
        }
    }

    public Seq<DetectedChange> getToDelete(long customerId, String clientName, String jobId, List<String> campaignIds) {
        return seq(digestDao.getEntitiesToDelete(customerId, jobId, campaignIds, clientName))
                .map(id -> new DetectedChange(id, Optional.empty(), Optional.empty(), DELETED));
    }

    private Set<AdGroupLevelId> notSameDigestInDB(long customerId, String clientName, String jobId, int bulkId) {
        return digestDao.getChangedDigestsForBulk(customerId, jobId, bulkId, clientName);
    }

}
