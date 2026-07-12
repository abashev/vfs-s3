package com.github.vfss3.spring;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Suite F: ACL — see {@code docs/test-cases/f-acl.md}.
 *
 * <p>The Spring {@link org.springframework.core.io.Resource} abstraction has no concept of S3
 * ACLs, so the whole suite is not applicable to this module and is disabled.
 */
final class AclTest {

    @Disabled("S3 ACLs are not exposed through the Spring Resource API")
    @DisplayName("ACL operations are not applicable to the Spring Resource module")
    @Test
    void aclNotApplicable() {
        // Intentionally empty — see class javadoc.
    }
}
