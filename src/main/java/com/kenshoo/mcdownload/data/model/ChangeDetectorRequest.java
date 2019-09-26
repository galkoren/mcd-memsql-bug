package com.kenshoo.mcdownload.data.model;

public class ChangeDetectorRequest<ID> {

    private ChangeDetectorRequest(byte[] digest, byte[] serializedEntity, ID id) {
        this.digest = digest;
        this.serializedEntity = serializedEntity;
        this.id = id;
    }

    private final byte[] digest;
    private final byte[] serializedEntity;
    private final ID id;

    public byte[] getDigest() {
        return digest;
    }

    public byte[] getSerializedEntity() {
        return serializedEntity;
    }

    public ID getId() {
        return id;
    }

    public static class Builder<ID> {
        private byte[] digest;
        private byte[] serializedEntity;
        private ID id;

        public Builder<ID> setDigest(byte[] digest) {
            this.digest = digest;
            return this;
        }

        public Builder<ID> setSerializedEntity(byte[] serializedEntity) {
            this.serializedEntity = serializedEntity;
            return this;
        }

        public Builder<ID> setId(ID id) {
            this.id = id;
            return this;
        }

        public ChangeDetectorRequest<ID> build() {
            return new ChangeDetectorRequest<>(digest, serializedEntity, id);
        }
    }

}
