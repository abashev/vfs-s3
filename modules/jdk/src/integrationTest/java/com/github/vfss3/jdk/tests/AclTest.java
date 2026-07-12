package com.github.vfss3.jdk.tests;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite F: ACL — see {@code docs/test-cases/f-acl.md}.
 *
 * <p>The JDK NIO.2 file-system API has no analog for S3 object/bucket ACLs (no {@code
 * AclFileAttributeView} is exposed), so the whole suite is not applicable to this module and is
 * disabled.
 */
final class AclTest {

    @Disabled("S3 ACLs are not exposed through the JDK NIO.2 API")
    @DisplayName("ACL operations are not applicable to the NIO.2 module")
    @Test
    void aclNotApplicable() {
        // Intentionally empty — see class javadoc.
    }
}
