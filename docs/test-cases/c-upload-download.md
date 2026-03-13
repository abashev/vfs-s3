# Suite C: Upload & Download

Prefix: `/upload/`

## Steps

```
1. Copy local backup.zip to /upload/backup.zip
   → assert: exists, type is FILE

2. Copy local backup.zip to /upload/backup.zip again (overwrite after 2s wait)
   → assert: exists, type is FILE

3. Read local backup.zip into byte array
   → open OutputStream to /upload/output.txt, write bytes, close
   → assert: exists, type is FILE, size matches local
   → open InputStream from /upload/output.txt, read all
   → assert: content matches original byte array
   → delete /upload/output.txt

4. Copy local backup.zip to /upload/deep/sub1/sub2/backup.zip
   → assert: file exists, type is FILE
   → assert: /upload/deep/sub1 exists, type is FOLDER
   → assert: /upload/deep/sub1/sub2 exists, type is FOLDER

5. Open InputStream from /upload/backup.zip, copy to local temp file
   → assert: remote size == temp file size
   → delete temp file
```

## Teardown

Delete `/upload/` recursively.

## Dependencies

Step 2 depends on step 1. Step 3 is independent. Step 4 is independent. Step 5 depends on step 1.
