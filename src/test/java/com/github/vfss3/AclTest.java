package com.github.vfss3;

import com.github.vfss3.operations.Acl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AclTest {
    @Test
    public void testOperations () {
        Acl acl = new Acl();
        acl.denyAll();

        // Allow single right
        acl.allow(Acl.Group.OWNER, Acl.Permission.WRITE);
        Assert.assertTrue(acl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Deny single right
        acl.deny(Acl.Group.OWNER, Acl.Permission.WRITE);
        Assert.assertTrue(acl.isDenied(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Allow rights
        Acl.Permission[] rights = {Acl.Permission.READ, Acl.Permission.WRITE};
        acl.allow(Acl.Group.AUTHORIZED, rights);
        Assert.assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Deny rights
        acl.deny(Acl.Group.AUTHORIZED, rights);
        Assert.assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Allow all to group
        acl.allow(Acl.Group.AUTHORIZED);
        Assert.assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Deny all to group
        acl.deny(Acl.Group.AUTHORIZED);
        Assert.assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.READ));

        // Allow all to all
        acl.allowAll();
        Assert.assertTrue(acl.isAllowed(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isAllowed(Acl.Group.EVERYONE, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isAllowed(Acl.Group.OWNER, Acl.Permission.WRITE));

        // Deny all to all
        acl.denyAll();
        Assert.assertTrue(acl.isDenied(Acl.Group.AUTHORIZED, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isDenied(Acl.Group.EVERYONE, Acl.Permission.WRITE));
        Assert.assertTrue(acl.isDenied(Acl.Group.OWNER, Acl.Permission.WRITE));
    }
}
