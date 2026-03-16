package com.github.vfss3.commonsvfs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.vfss3.commonsvfs.operations.Acl;
import org.junit.jupiter.api.Test;

public class AclTest {
    @Test
    public void testOperations() {
        Acl acl = new Acl();
        acl.denyAll();

        // Allow single right
        acl.allow(Acl.Group.OWNER, Acl.Permission.WRITE);
        assertTrue(acl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Deny single right
        acl.deny(Acl.Group.OWNER, Acl.Permission.WRITE);
        assertTrue(acl.isDenied(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Allow rights
        Acl.Permission[] rights = {Acl.Permission.READ, Acl.Permission.WRITE};
        acl.allow(Acl.Group.AUTHORIZED, rights);
        assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Deny rights
        acl.deny(Acl.Group.AUTHORIZED, rights);
        assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Allow all to group
        acl.allow(Acl.Group.AUTHORIZED);
        assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Deny all to group
        acl.deny(Acl.Group.AUTHORIZED);
        assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Allow all to all
        acl.allowAll();
        assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE));
        assertTrue(acl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Deny all to all
        acl.denyAll();
        assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        assertTrue(acl.isDenied(Acl.Group.EVERYONE, Acl.Permission.WRITE));
        assertTrue(acl.isDenied(Acl.Group.OWNER, Acl.Permission.WRITE));
    }
}
