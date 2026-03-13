# Suite G: Concurrent Access

Prefix: `/concurrent/`

## Setup

```
Create folder /concurrent/folders/
Create folder /concurrent/read-test/
Create file /concurrent/read-test/file1
Create file /concurrent/read-test/file2
```

## Steps

```
1. Concurrent create/delete (200 iterations, parallel threads):
   Each iteration:
     → create folder /concurrent/folders/folder-<threadId>-<random>/
     → assert: exists
     → refresh → assert: still exists
     → delete
     → refresh → assert: does not exist

2. Concurrent read (200 iterations, parallel threads):
   Each iteration:
     → resolve /concurrent/read-test
     → getParent → assert: not null
     → refresh
     → getChildren → assert: not null
     → assert: exists

3. Deadlock detection:
   → delete children of /concurrent/folders/
   → create 10 files: deadlock-0 through deadlock-9
   → start 1 thread: continuously getParent() for each file
   → start 3 threads: continuously getChildren(), verify count == 10
   → run 5 seconds, check for deadlocks every 1 second
   → assert: no deadlocks detected, 0 wrong results
   → clean up children
```

## Teardown

Delete `/concurrent/` recursively.

## Dependencies

Steps 1 and 2 are independent. Step 3 depends on step 1 completing (reuses same prefix).
