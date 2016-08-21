package com.intridea.io.vfs.operations;

import java.util.*;

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
 * @deprecated Use {@link com.github.vfss3.operations.Acl}
 */
@Deprecated
public class Acl {
    @Deprecated
    public enum Permission {
        READ, WRITE;

        com.github.vfss3.operations.Acl.Permission unwrap() {
            switch (this) {
                case READ : return com.github.vfss3.operations.Acl.Permission.READ;
                case WRITE: return com.github.vfss3.operations.Acl.Permission.WRITE;
            }

            throw new IllegalStateException("Wrong state for deprecated permission - " + this);
        }

        static Permission wrap(com.github.vfss3.operations.Acl.Permission permission) {
            switch (permission) {
                case READ : return READ;
                case WRITE: return WRITE;
            }

            throw new IllegalStateException("Wrong state for deprecated permission - " + permission);
        }
    }

    @Deprecated
    public enum Group {
        OWNER, AUTHORIZED, EVERYONE;

        com.github.vfss3.operations.Acl.Group unwrap() {
            switch (this) {
                case OWNER: return com.github.vfss3.operations.Acl.Group.OWNER;
                case AUTHORIZED: return com.github.vfss3.operations.Acl.Group.AUTHORIZED;
                case EVERYONE: return com.github.vfss3.operations.Acl.Group.EVERYONE;
            }

            throw new IllegalStateException("Wrong state for deprecated group - " + this);
        }

        static Group wrap(com.github.vfss3.operations.Acl.Group group) {
            switch (group) {
                case OWNER: return OWNER;
                case AUTHORIZED: return AUTHORIZED;
                case EVERYONE: return EVERYONE;
            }

            throw new IllegalStateException("Wrong state for deprecated group - " + group);
        }
    };

    private final com.github.vfss3.operations.Acl acl;

    /**
     * Create new empty ACL.
     */
    public Acl() {
        this.acl = new com.github.vfss3.operations.Acl();
    }

    /**
     * Create ACL and load rules.
     * @param rules A set of predefined rules.
     */
    public Acl(Map<Group, Permission[]> rules) {
        this.acl = new com.github.vfss3.operations.Acl(unwrapMap(rules));
    }

    /**
     * Allow access for a group
     * @param group
     * @param permission
     */
    public void allow (Group group, Permission permission) {
        acl.allow(group.unwrap(), permission.unwrap());
    }

    /**
     * Set a list of permissions for a group.
     * @param group
     * @param permission_list
     */
    public void allow (Group group, Permission[] permission_list) {
        acl.allow(group.unwrap(), unwrapArray(permission_list));
    }

    /**
     * Allow all permissions for a group
     * @param group
     */
    public void allow (Group group) {
        acl.allow(group.unwrap());
    }

    /**
     * Allow specific permission for all
     */
    public void allow (Permission permission) {
        acl.allow(permission.unwrap());
    }

    /**
     * Allow access for all
     */
    public void allow (Permission[] permission_list) {
        acl.allow(unwrapArray(permission_list));
    }

    /**
     * Allow all to all
     */
    public void allowAll () {
        acl.allowAll();
    }

    /**
     * Deny right to group
     */
    public void deny (Group group, Permission permission) {
        acl.deny(group.unwrap(), permission.unwrap());
    }

    /**
     * Deny access for a group
     */
    public void deny (Group group, Permission[] permission_list) {
        acl.deny(group.unwrap(), unwrapArray(permission_list));
    }

    /**
     * Deny all to for a group
     * @param group
     */
    public void deny (Group group) {
        acl.deny(group.unwrap());
    }

    /**
     * Deny access for all
     */
    public void deny (Permission permission) {
        acl.deny(permission.unwrap());
    }

    /**
     * Deny access for all
     */
    public void deny (Permission[] permission) {
        acl.deny(unwrapArray(permission));
    }

    /**
     * Completely deny.
     */
    public void denyAll () {
        acl.denyAll();
    }

    /**
     * Returns true when a group has specific access.
     */
    public boolean isAllowed (Group group, Permission permission) {
        return acl.isAllowed(group.unwrap(), permission.unwrap());
    }

    /**
     * Returns true when specific access is denied for a group
     */
    public boolean isDenied (Group group, Permission permission) {
        return acl.isDenied(group.unwrap(), permission.unwrap());
    }

    /**
     * Sets a list of allowed rules. Calls {@link #denyAll()} and then applies rules.
     *
     * @param rules Access rule to apply.
     */
    public void setRules(Map<Group, Permission[]> rules) {
        acl.setRules(unwrapMap(rules));
    }

    /**
     * Returns a list of allowed rules.
     *
     * @return
     */
    public Map<Group, Permission[]> getRules() {
        return wrapMap(acl.getRules());
    }

    @Override
    public String toString() {
        return acl.toString();
    }

    public com.github.vfss3.operations.Acl unwrap() {
        return acl;
    }

    public static Acl wrap(com.github.vfss3.operations.Acl acl) {
        return new Acl(wrapMap(acl.getRules()));
    }

    private static com.github.vfss3.operations.Acl.Permission[] unwrapArray(Permission[] permissions) {
        com.github.vfss3.operations.Acl.Permission[] result = new com.github.vfss3.operations.Acl.Permission[permissions.length];

        for (int i = 0; i < permissions.length; i++) {
            result[i] = permissions[i].unwrap();
        }

        return result;
    }

    private static Permission[] wrapArray(com.github.vfss3.operations.Acl.Permission[] permissions) {
        Permission[] result = new Permission[permissions.length];

        for (int i = 0; i < permissions.length; i++) {
            result[i] = Permission.wrap(permissions[i]);
        }

        return result;
    }

    private static Map<com.github.vfss3.operations.Acl.Group, com.github.vfss3.operations.Acl.Permission[]> unwrapMap(Map<Group, Permission[]> map) {
        if (map == null) {
            return null;
        }

        Map<com.github.vfss3.operations.Acl.Group, com.github.vfss3.operations.Acl.Permission[]> result = new HashMap<>(map.size());

        for (Map.Entry<Group, Permission[]> entry : map.entrySet()) {
            result.put(entry.getKey().unwrap(), unwrapArray(entry.getValue()));
        }

        return result;
    }

    private static Map<Group, Permission[]> wrapMap(Map<com.github.vfss3.operations.Acl.Group, com.github.vfss3.operations.Acl.Permission[]> map) {
        if (map == null) {
            return null;
        }

        Map<Group, Permission[]> result = new HashMap<>(map.size());

        for (Map.Entry<com.github.vfss3.operations.Acl.Group, com.github.vfss3.operations.Acl.Permission[]> entry : map.entrySet()) {
            result.put(Group.wrap(entry.getKey()), wrapArray(entry.getValue()));
        }

        return result;
    }
}
