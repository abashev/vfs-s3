package com.github.vfss3.commonsvfs.operations;

import org.apache.commons.vfs2.operations.FileOperation;

/**
 * Different S3 implementations have slightly different behaviour, and we need to control it on code side
 *
 * @author <A href="mailto:alexey@abashev.ru">Alexey Abashev</A>
 */
public interface PlatformFeatures extends FileOperation {
    /**
     * Default permission for owner - everybody have `allowed`, but Yandex deny for everybody.
     *
     * @return {@code true} if the owner is allowed by default
     */
    boolean defaultAllowForOwner();

    /**
     * Mail.ru implementation doesn't allow to deny access for an owner.
     *
     * @return {@code true} if access may be denied for the owner
     */
    boolean allowDenyForOwner();

    /**
     * Does it support server-side encryption
     *
     * @return {@code true} if the backend supports server-side encryption
     */
    boolean supportsServerSideEncryption();

    /**
     * Does it support special group for authorized requests - For AliCloud it is only Owner and Everyone
     *
     * @return {@code true} if the backend supports the authorized-requests group
     */
    boolean supportsAuthorizedGroup();

    /**
     * Does it support Acl for files.
     *
     * @return {@code true} if the backend supports per-file ACLs
     */
    boolean supportsAcl();
}
