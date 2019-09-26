package com.kenshoo.mcdownload.data.model;


import com.google.common.base.Objects;

public class AdGroupLevelId {

    private long campId;
    private long adgroupId;
    private long entityId;


    public AdGroupLevelId(long campId, long adgroupId, long entityId) {
        this.campId = campId;
        this.adgroupId = adgroupId;
        this.entityId = entityId;
    }

    public long getCampId() {
        return campId;
    }

    public long getAdgroupId() {
        return adgroupId;
    }

    public long getEntityId() {
        return entityId;
    }


    @Override
    public String toString() {
        return campId + "\t" + adgroupId + "\t" + entityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdGroupLevelId that = (AdGroupLevelId) o;
        return campId == that.campId &&
                adgroupId == that.adgroupId &&
                entityId == that.entityId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(campId, adgroupId, entityId);
    }


    public static class Builder {
        private long campId;
        private Long adgroupId;
        private long entityId;

        public Builder setCampaignID(long campaignID) {
            this.campId = campaignID;
            return this;
        }

        public Builder setAdGroupId(Long adGroupId) {
            this.adgroupId = adGroupId;
            return this;
        }

        public Builder setEntityId(long entityId) {
            this.entityId = entityId;
            return this;
        }

        public AdGroupLevelId build() {
            return new AdGroupLevelId(campId, adgroupId, entityId);
        }
    }
}
