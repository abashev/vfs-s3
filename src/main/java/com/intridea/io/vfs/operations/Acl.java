package com.intridea.io.vfs.operations;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * ACL for S3.
 * Usage samples:
 * Allow everyone to read:
 * <code>
 * acl.allow(Acl.Group.GUEST, Acl.Permission.READ);
 * </code>
 * Allow all authorized users to read and write:
 * <code>
 * Acl.Permission[] rights = {Acl.Permission.READ, Acl.Permission.WRITE};
 * acl.allow(Acl.Group.AUTHORIZED, rights);
 * </code>
 * Deny all (owner still has access to overwrite ACL):
 * <code>
 * acl.denyAll();
 * </code>
 *
 * @author Marat Komarov
 * @author Alex Kovalyov <alex@intridea.com>
 *
 */
public class Acl {
    public static enum Permission {
        READ, WRITE
    }

    public static enum Group {
        OWNER, AUTHORIZED, EVERYONE
    };

    /**
     * Number of available groups
     */
    private int groupsCount = Group.values().length;

    /**
     * Number of available rules
     */
    private int rulesCount = Permission.values().length;

    /**
     * Internal rights holder
     */
    private byte[][] rulesTable;

    /**
     * @see {@link #getRules()}
     */
    private Hashtable<Group, Permission[]> rules;

    /**
     * Will be True if rules were changed since last {@link #getRules()} call.
     */
    private boolean changed = true;

    /**
     * Create new empty ACL.
     */
    public Acl () {
        this(null);
    }

    /**
     * Create ACL and load rules.
     * @param rules A set of predefined rules.
     */
    public Acl (Hashtable<Group, Permission[]> rules) {
        rulesTable = new byte[groupsCount][rulesCount];
        if (rules != null) {
            setRules(rules);
        } else {
            denyAll();
        }
    }

    /**
     * Allow access for a group
     * @param group
     * @param permission
     */
    public void allow (Group group, Permission permission) {
        setRule(group, permission, (byte) 1);
    }

    /**
     * Set a list of permissions for a group.
     * @param group
     * @param permission_list
     */
    public void allow (Group group, Permission[] permission_list) {
        setRule(group, permission_list, (byte) 1);
    }

    /**
     * Allow all permissions for a group
     * @param group
     */
    public void allow (Group group) {
        setRule(group, (byte) 1);
    }

    /**
     * Allow specific permission for all
     * @param right
     */
    public void allow (Permission permission) {
        setRule(permission, (byte) 1);
    }

    /**
     * Allow access for all
     * @param access_list
     */
    public void allow (Permission[] permission_list) {
        setRule(permission_list, (byte) 1);
    }

    /**
     * Allow all to all
     */
    public void allowAll () {
        setRule((byte) 1);
    }


    /**
     * Deny right to group
     * @param group
     * @param access
     */
    public void deny (Group group, Permission permission) {
        setRule(group, permission, (byte) 0);
    }

    /**
     * Deny access for a group
     * @param group
     * @param right
     */
    public void deny (Group group, Permission[] permission_list) {
        setRule(group, permission_list, (byte) 0);
    }

    /**
     * Deny all to for a group
     * @param group
     */
    public void deny (Group group) {
        setRule(group, (byte) 0);
    }

    /**
     * Deny access for all
     * @param access
     */
    public void deny (Permission permission) {
        setRule(permission, (byte) 0);
    }

    /**
     * Deny access for all
     * @param access
     */
    public void deny (Permission[] permission) {
        setRule(permission, (byte) 0);
    }

    /**
     * Completely deny.
     */
    public void denyAll () {
        setRule((byte) 0);
    }

    /**
     * Returns true when a group has specific access.
     *
     * @param group
     * @param access
     * @return
     */
    public boolean isAllowed (Group group, Permission permission) {
        return rulesTable[group.ordinal()][permission.ordinal()] == 1;
    }

    /**
     * Returns true when specific access is denied for a group
     *
     * @param group
     * @param access
     * @return
     */
    public boolean isDenied (Group group, Permission permission) {
        return rulesTable[group.ordinal()][permission.ordinal()] == 0;
    }

    /**
     * Sets a list of allowed rules. Calls {@link #denyAll()} and then applies rules.
     *
     * @param rules Access rule to apply.
     */
    public void setRules (Hashtable<Group, Permission[]> rules) {
        // Deny all by default
        denyAll();

        // Set allow rules
        Enumeration<Group> en = rules.keys();
        while (en.hasMoreElements()) {
            Group group = en.nextElement();
            Permission[] permissions = rules.get(group);
            allow(group, permissions);
        }
    }

    /**
     * Returns a list of allowed rules.
     *
     * @return
     */
    public Hashtable<Group, Permission[]> getRules () {
        if (changed) {
            rules = new Hashtable<Group, Permission[]>(groupsCount);
            Permission[] rightValues = Permission.values();
            for (Group group : Group.values()) {
                int groupIndex = group.ordinal();
                Vector<Permission> permissions = new Vector<Permission>();
                for (int i=0; i<rulesCount; i++) {
                    if (rulesTable[groupIndex][i] == 1) {
                        permissions.add(rightValues[i]);
                    }
                }
                rules.put(group, permissions.toArray(new Permission[0]));
            }
        }
        return rules;
    }

    /*
     * Helper methods
     *
     */

    private void setRule (Group group, Permission permission, byte allow) {
        rulesTable[group.ordinal()][permission.ordinal()] = allow;
        changed = true;
    }

    private void setRule (Group group, Permission[] permissions, byte allow) {
        int groupIndex = group.ordinal();
        for (Permission permission : permissions) {
            rulesTable[groupIndex][permission.ordinal()] = allow;
        }
        changed = true;
    }

    private void setRule (Group group, byte allow) {
        int groupIndex = group.ordinal();
        for (int i=0; i<rulesCount; i++) {
            rulesTable[groupIndex][i] = allow;
        }
        changed = true;
    }

    private void setRule (Permission permission, byte allow) {
        int i = permission.ordinal();
        for (int j=0; j<groupsCount; j++) {
            rulesTable[j][i] = allow;
        }
    }

    private void setRule (Permission[] permissions, byte allow) {
        for (Permission permission : permissions) {
            int i = permission.ordinal();
            for (int j=0; j<groupsCount; j++) {
                rulesTable[j][i] = allow;
            }
        }
    }

    private void setRule (byte allow) {
        for (int j=0; j<groupsCount; j++) {
            for (int i=0; i<rulesCount; i++) {
                rulesTable[j][i] = allow;
            }
        }
        changed = true;
    }
}
