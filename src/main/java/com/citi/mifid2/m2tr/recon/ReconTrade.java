package com.citi.mifid2.m2tr.recon;

import java.util.Objects;

public class ReconTrade implements Comparable<ReconTrade> {
    private Integer sourceId;
    private Integer sourceUId;
    private Integer sourceVersion;

    public ReconTrade() {}

    public ReconTrade(Integer sourceId, Integer sourceUId, Integer sourceVersion) {
        this.sourceId = sourceId;
        this.sourceUId = sourceUId;
        this.sourceVersion = sourceVersion;
    }

    public Integer getSourceId() {
        return sourceId;
    }

    public void setSourceId(Integer sourceId) {
        this.sourceId = sourceId;
    }

    public Integer getSourceUId() {
        return sourceUId;
    }

    public void setSourceUId(Integer sourceUId) {
        this.sourceUId = sourceUId;
    }

    public Integer getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(Integer sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReconTrade that = (ReconTrade) o;
        return Objects.equals(sourceId, that.sourceId) && Objects.equals(sourceUId, that.sourceUId) && Objects.equals(sourceVersion, that.sourceVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, sourceUId, sourceVersion);
    }

    @Override
    public int compareTo(ReconTrade o) {
        if(this.sourceId.compareTo(o.getSourceId()) == 0) {
            if(this.sourceUId.compareTo(o.getSourceUId()) == 0) {
                return this.sourceVersion.compareTo(o.getSourceVersion());
            } else {
                return this.sourceUId.compareTo(o.getSourceUId());
            }
        }
        else {
            return this.sourceId.compareTo(o.getSourceId());
        }
    }

    @Override
    public String toString() {
        return "ReconTrade{" +
                "sourceId='" + sourceId + '\'' +
                ", sourceUId='" + sourceUId + '\'' +
                ", sourceVersion=" + sourceVersion +
                '}';
    }
}
