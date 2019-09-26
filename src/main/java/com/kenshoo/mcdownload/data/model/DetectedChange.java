package com.kenshoo.mcdownload.data.model;

import java.util.Optional;

public class DetectedChange {
    public DetectedChange(AdGroupLevelId id, Optional<byte[]> digest, Optional<byte[]> entityBlob, ChangeType changeType) {
        this.id = id;
        this.digest = digest;
        this.entityBlob = entityBlob;
        this.changeType = changeType;
    }

    public AdGroupLevelId getId() {
        return id;
    }

    public Optional<byte[]> getDigest() {
        return digest;
    }

    public Optional<byte[]> getEntityBlob() {
        return entityBlob;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    private final AdGroupLevelId id;
    private final Optional<byte[]> digest;
    private final Optional<byte[]> entityBlob;
    private final ChangeType changeType;

}
