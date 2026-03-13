# Suite E: Copy Operations

Prefix: `/copy/`

## Steps

```
1. Create structure under /copy/:
     child-file.tmp
     child-file2.tmp
     child-dir/
     child-dir/descendant.tmp
     child-dir/descendant2.tmp
     child-dir/descendant-dir/
   → verify: SELECT_CHILDREN=3, SELECT_FOLDERS=3, SELECT_FILES=4, EXCLUDE_SELF=6

2. Copy /copy/child-dir to /copy/child-dir-copy (SELECT_SELF_AND_CHILDREN)
   → assert: /copy/child-dir-copy exists
   → assert: same number of files in source and copy

3. List children of /copy/
   → assert: contains child-file.tmp, child-file2.tmp, child-dir, child-dir-copy

4. Delete all children of /copy/ (EXCLUDE_SELF)
   → assert: deleted count > 0
   → assert: /copy/child-dir does not exist
```

## Teardown

Delete `/copy/` recursively.

## Dependencies

Step 2 depends on step 1. Step 3 depends on step 2. Step 4 depends on step 3.
