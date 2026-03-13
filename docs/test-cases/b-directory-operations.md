# Suite B: Directory Operations

Prefix: `/dir-ops/`

## Steps

```
1. Create folder at /dir-ops/my-folder
   → assert: exists, type is FOLDER

2. Resolve /dir-ops/my-folder (existing folder)
   → try createFile at same path → assert: throws error

3. Create folder at /dir-ops/folder with space
   → assert: exists

4. Create 5 files inside /dir-ops/my-folder/:
     0.tmp, 1.tmp, 2.tmp, 3.tmp, 4.tmp
   → getChildren of /dir-ops/my-folder/
   → assert: 5 children

5. Create nested structure under /dir-ops/find-tests/:
     child-file.tmp
     child-file2.tmp
     child-dir/
     child-dir/descendant.tmp
     child-dir/descendant2.tmp
     child-dir/descendant-dir/
   → find SELECT_CHILDREN → assert: 3
   → find SELECT_FOLDERS → assert: 3
   → find SELECT_FILES → assert: 4
   → find EXCLUDE_SELF → assert: 6

6. Delete children of /dir-ops/find-tests/ (EXCLUDE_SELF)
   → find SELECT_ALL → assert: 1 (only the folder itself)
```

## Teardown

Delete `/dir-ops/` recursively.

## Dependencies

Step 2 depends on step 1. Steps 3–5 are independent of each other. Step 6 depends on step 5.
