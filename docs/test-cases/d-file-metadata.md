# Suite D: File Metadata

Prefix: `/metadata/`

## Setup

Copy local backup.zip to `/metadata/backup.zip`.

## Steps

```
1. Get content type of /metadata/backup.zip
   → assert: "application/zip"

2. Get content size of /metadata/backup.zip
   → assert: 996,166 bytes

3. Get lastModifiedTime of /metadata/backup.zip
   → convert to year (UTC)
   → assert: year > 2010

4. Get IPublicUrlsGetter for /metadata/backup.zip
   → get HTTP URL → assert: contains "https" and "/metadata/backup.zip"
   → get signed URL (60s) → assert: contains "Signature=", "X-Amz-Credential="

5. Get IMD5HashGetter for /metadata/backup.zip
   → get remote MD5
   → compute local MD5 of backup.zip
   → assert: match (case-insensitive)
```

## Teardown

Delete `/metadata/` recursively.

## Dependencies

All steps depend only on Setup. Steps 1–5 are independent of each other and could be parallelized.
