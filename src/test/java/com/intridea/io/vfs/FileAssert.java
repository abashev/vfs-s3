package com.intridea.io.vfs;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Arrays.sort;
import static org.testng.Assert.*;

/**
 * A bunch of asserts for checking file operations.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public final class FileAssert {
    /**
     * Check list of children for file object. Number of children and names should exactly as `children` param.
     *
     * @param file
     * @param children
     */
    public static void assertHasChildren(FileObject file, String ... children) {
        assertNotNull(file, "Source file object is null");

        FileObject[] siblings = null;

        try {
            siblings = file.getChildren();
        } catch (FileSystemException e) {
            fail("Not able to get children for " + file, e);
        }

        sort(children);

        Set<String> names = new TreeSet<>();

        for (FileObject sibling : siblings) {
            names.add(sibling.getName().getBaseName());
        }

        if (names.size() != children.length) {
            fail(
                    "Wrong number of children for " + file +
                            ". Expected <" + Arrays.toString(children) +
                            "> but was <" + names.toString() + ">"
            );
        }

        int i = 0;

        for (String name : names) {
            if (!name.equals(children[i++])) {
                fail(
                        "Wrong list of children for " + file +
                        ". Expected <" + Arrays.toString(children) +
                        "> but was <" + names.toString() + ">"
                );
            }
        }
    }

    private FileAssert() {
    }
}
