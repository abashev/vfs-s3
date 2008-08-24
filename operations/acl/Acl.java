package com.intridea.io.vfs.operations.acl;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * An access control list for VFS. Focuses on network storage services like Amazon S3.
 * Rules is as a set of Acl.Group and Acl.Right.
 * Group, Right = Rule
 * 
 * There are 3 groups:
 * <ul>
 * 	<li>Acl.Group.OWNER - The owner of the file</li>
 *  <li>Acl.Group.AUTHORIZED - Authorized users of storage service</li>
 *  <li>Acl.Group.GUEST - Anonimous users</li>
 * </ul>
 * 
 * There are 2 rights:
 * <ul>
 * 	<li>Acl.Right.READ - grants permisiion to read file or list contents of directory</li>
 *  <li>Acl.Right.WRITE - grants permission to create/overwrite/delete file or directory</li>
 * </ul>
 * 
 * Rights can be applied in very flexible way:
 * A single right to group:
 * <code>
 * acl.allow(Acl.Group.GUEST, Acl.Right.READ);
 * </code>
 * A set of rights to group:
 * <code>
 * Acl.Right[] rights = {Acl.Right.READ, Acl.Right.WRITE};
 * acl.allow(Acl.Group.AUTHORIZED, rights);
 * </code>
 * A single right to all groups
 * <code>
 * acl.allow(Acl.Right.READ);
 * acl.deny(Acl.Right.WRITE);
 * </code>
 * You can allow or deny all
 * <code>
 * acl.allowAll();
 * acl.denyAll();
 * </code>
 * 
 * @author Marat Komarov
 *
 */
public class Acl {
	public static enum Right {
		READ, WRITE
	}
	
	public static enum Group {
		OWNER, AUTHORIZED, GUEST
	};
	
	/**
	 * Number of available groups 
	 */
	private int groupsNum = Group.values().length;
	
	/**
	 * Number of available rights
	 */
	private int rightsNum = Right.values().length;

	/**
	 * Internal rights storage
	 */
	private byte[][] rightsTable;	
	
	/**
	 * Rules presentation for {@link #getRules()} call
	 */
	private Hashtable<Group, Right[]> rules;
	
	/**
	 * True when rules changed sinse last {@link #getRules()} call
	 */
	private boolean changed = true;
	
	public Acl () {
		this(null);
	}
	
	public Acl (Hashtable<Group, Right[]> rules) {
		rightsTable = new byte[groupsNum][rightsNum];
		if (rules != null) {
			setRules(rules);
		} else {
			denyAll();
		}
	}
	
	/**
	 * Allow right to group
	 * @param group
	 * @param right
	 */
	public void allow (Group group, Right right) {
		setRule(group, right, (byte) 1);
	}
	
	/**
	 * Allow list of rights to group
	 * @param group
	 * @param right
	 */
	public void allow (Group group, Right[] rights) {
		setRule(group, rights, (byte) 1);
	}
	
	/**
	 * Allow all to group
	 * @param group
	 */
	public void allow (Group group) {
		setRule(group, (byte) 1);
	}

	/**
	 * Allow right to all
	 * @param right
	 */
	public void allow (Right right) {
		setRule(right, (byte) 1);
	}
	
	/**
	 * Allow rights to all
	 * @param rights
	 */
	public void allow (Right[] rights) {
		setRule(rights, (byte) 1);
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
	 * @param right
	 */
	public void deny (Group group, Right right) {
		setRule(group, right, (byte) 0);
	}
	
	/**
	 * Deny list of rights to group
	 * @param group
	 * @param right
	 */
	public void deny (Group group, Right[] rights) {
		setRule(group, rights, (byte) 0);
	}
	
	/**
	 * Deny all to group
	 * @param group
	 */
	public void deny (Group group) {
		setRule(group, (byte) 0);
	}
	
	/**
	 * Deny right to all
	 * @param right
	 */
	public void deny (Right right) {
		setRule(right, (byte) 0);
	}	

	/**
	 * Deny rights to all
	 * @param right
	 */
	public void deny (Right[] right) {
		setRule(right, (byte) 0);
	}	
	
	/**
	 * Deny all to all
	 */
	public void denyAll () {
		setRule((byte) 0);
	}	
	
	/**
	 * Returns true when right is allowed to group.
	 * 
	 * @param group
	 * @param right
	 * @return
	 */
	public boolean isAllowed (Group group, Right right) {
		return rightsTable[group.ordinal()][right.ordinal()] == 1;
	}
	
	/**
	 * Returns true when right is not allowed to group.
	 * 
	 * @param group
	 * @param right
	 * @return
	 */
	public boolean isDenied (Group group, Right right) {
		return rightsTable[group.ordinal()][right.ordinal()] == 0;
	}

	/**
	 * Sets a list of allowed rules. All is not allowed -- DENIED.
	 * 
	 * @param rules
	 */
	public void setRules (Hashtable<Group, Right[]> rules) {
		// Deny all by default
		denyAll();
		
		// Set allow rules
		Enumeration<Group> en = rules.keys();
		while (en.hasMoreElements()) {
			Group group = en.nextElement();
			Right[] rights = rules.get(group);
			allow(group, rights);
		}
	}
	
	/**
	 * Returns a list of allowed rules. All is not allowed -- DENIED.
	 * 
	 * @return
	 */
	public Hashtable<Group, Right[]> getRules () {
		if (changed) {
			rules = new Hashtable<Group, Right[]>(groupsNum);
			Right[] rightValues = Right.values();
			for (Group group : Group.values()) {
				int groupIndex = group.ordinal();
				Vector<Right> rights = new Vector<Right>();
				for (int i=0; i<rightsNum; i++) {
					if (rightsTable[groupIndex][i] == 1) {
						rights.add(rightValues[i]);
					}
				}
				rules.put(group, rights.toArray(new Right[0]));
			}
		}
		return rules;
	}
	
	// Utilities methods
	
	private void setRule (Group group, Right right, byte allow) {
		rightsTable[group.ordinal()][right.ordinal()] = allow;
		changed = true;
	}
	
	private void setRule (Group group, Right[] rights, byte allow) {
		int groupIndex = group.ordinal();
		for (Right right : rights) {
			rightsTable[groupIndex][right.ordinal()] = allow;
		}
		changed = true;		
	}	
	
	private void setRule (Group group, byte allow) {
		int groupIndex = group.ordinal();
		for (int i=0; i<rightsNum; i++) {
			rightsTable[groupIndex][i] = allow;
		}
		changed = true;
	}	
	
	private void setRule (Right right, byte allow) {
		int rightIndex = right.ordinal();
		for (int j=0; j<groupsNum; j++) {
			rightsTable[j][rightIndex] = allow;
		}
	}
	
	private void setRule (Right[] rights, byte allow) {
		for (Right right : rights) {
			int rightIndex = right.ordinal();
			for (int j=0; j<groupsNum; j++) {
				rightsTable[j][rightIndex] = allow;
			}
		}
	}
	
	private void setRule (byte allow) {
		for (int j=0; j<groupsNum; j++) {
			for (int i=0; i<rightsNum; i++) {
				rightsTable[j][i] = allow;
			}
		}
		changed = true;
	}
}
