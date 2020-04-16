package com.github.vfss3;

import com.github.vfss3.operations.PlatformFeatures;

import java.util.Objects;

/**
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
class PlatformFeaturesImpl implements PlatformFeatures {
    private final boolean defaultAllowForOwner;
    private final boolean allowDenyForOwner;
    private final boolean supportsServerSideEncryption;

    public PlatformFeaturesImpl(boolean defaultAllowForOwner, boolean allowDenyForOwner, boolean supportsServerSideEncryption) {
        this.defaultAllowForOwner = defaultAllowForOwner;
        this.allowDenyForOwner = allowDenyForOwner;
        this.supportsServerSideEncryption = supportsServerSideEncryption;
    }

    @Override
    public boolean defaultAllowForOwner() {
        return defaultAllowForOwner;
    }

    @Override
    public boolean allowDenyForOwner() {
        return allowDenyForOwner;
    }

    @Override
    public boolean supportsServerSideEncryption() {
        return supportsServerSideEncryption;
    }

    @Override
    public final void process() {
        // Nothing to do
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformFeaturesImpl that = (PlatformFeaturesImpl) o;
        return defaultAllowForOwner == that.defaultAllowForOwner &&
                allowDenyForOwner == that.allowDenyForOwner &&
                supportsServerSideEncryption == that.supportsServerSideEncryption;
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultAllowForOwner, allowDenyForOwner, supportsServerSideEncryption);
    }

    @Override
    public String toString() {
        return "PlatformFeaturesImpl{" +
                "defaultAllowForOwner=" + defaultAllowForOwner +
                ", allowDenyForOwner=" + allowDenyForOwner +
                ", supportsServerSideEncryption=" + supportsServerSideEncryption +
                '}';
    }
}
