package com.github.vfss3.samples;

import org.apache.commons.vfs2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class S3Shell {
    private final Logger log = LoggerFactory.getLogger(S3Shell.class);

    private FileSystemManager fsManager;

    public S3Shell() throws IOException {
        fsManager = VFS.getManager();
    }

    public void ls (String path) {
        try {
            FileObject[] files = fsManager.resolveFile(path).findFiles(Selectors.EXCLUDE_SELF);

            if ((files != null) && files.length > 0) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                for (FileObject file : files) {
                    String lastModDate = df.format(new Date(file.getContent().getLastModifiedTime()));
                    String fullPath = file.getName().getURI();

                    if (file.isFile()) {
                        long size = file.getContent().getSize();

                        System.out.println(String.format("%-20s%-10s%s", lastModDate, size, fullPath));
                    } else {
                        System.out.println(String.format("%-20s  -dir-   %s", lastModDate, fullPath));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Not able to list files from [{}]", path, e);
        }
    }


    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: s3Shell COMMAND [ARGS]");
            System.out.println("       s3Shell ls s3://mybucket");
            System.out.println("\nCurrently only 'ls' command supported.");
            System.exit(0);
        }

        S3Shell shell = null;

        try {
            shell = new S3Shell();
        } catch (Exception e) {
            System.out.println(String.format("Fail to init s3 shell. Reason: %s", e.getMessage()));
            e.printStackTrace();
            System.exit(1);
        }

        String cmd = args[0];

        if (cmd.equals("ls")) {
            shell.ls(args[1]);
        } else {
            System.out.println(String.format("Unknown command '%s'", cmd));
        }

        System.exit(0);
    }
}
