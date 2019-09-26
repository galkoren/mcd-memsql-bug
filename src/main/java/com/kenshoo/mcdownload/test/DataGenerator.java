package com.kenshoo.mcdownload.test;

import com.kenshoo.mcdownload.data.model.AdGroupLevelId;
import com.kenshoo.mcdownload.data.model.ChangeDetectorRequest;
import org.jooq.lambda.Seq;

public class DataGenerator {

    public static Seq<ChangeDetectorRequest<AdGroupLevelId>> generateFakeItems(long bulkSize) {
        return Seq.range(0, bulkSize).map(i -> new ChangeDetectorRequest.Builder<AdGroupLevelId>()
                .setDigest(new byte[] { (byte)(i % 10), (byte)(i+2 % 10) })
                .setSerializedEntity(/* bigByteArray(i) */ new byte[0])
                .setId(new AdGroupLevelId.Builder()
                        .setEntityId(i)
                        .setCampaignID(i % 1000)
                        .setAdGroupId((long)(i % 50))
                        .build())
                .build());
    }

}
