# Suite F: ACL

Prefix: `/acl/`

All steps check `PlatformFeatures.supportsAcl()` first and skip the entire suite if unsupported.

## Setup

Copy local backup.zip to `/acl/test-file.zip`.

## Steps

```
1. Get ACL of /acl/test-file.zip
   → assert: ACL not null
   → if defaultAllowForOwner: assert OWNER allowed READ+WRITE
   → if not: assert OWNER denied
   → assert: AUTHORIZED denied READ+WRITE
   → assert: EVERYONE denied READ+WRITE

2. Deny READ for EVERYONE
   → if allowDenyForOwner: also deny all for OWNER and AUTHORIZED
   → set ACL on the file
   → refresh, get updated ACL
   → if allowDenyForOwner: assert OWNER denied, AUTHORIZED denied
   → assert: EVERYONE denied

3. Create folder /acl/test-folder/
   → get ACL of the folder
   → if allowDenyForOwner: deny all
   → set ACL, refresh, get updated ACL
   → if allowDenyForOwner: assert OWNER, AUTHORIZED, EVERYONE all denied
```

## Teardown

Delete `/acl/` recursively.

## Dependencies

Step 2 depends on step 1. Step 3 is independent of steps 1–2.

## Platform Notes

ACL behavior differs between AWS S3 and S3-compatible providers (e.g., Yandex Object Storage):
- `defaultAllowForOwner`: AWS grants owner READ+WRITE by default; Yandex denies all
- `allowDenyForOwner`: whether the platform supports denying permissions for the object owner
- `supportsAuthorizedGroup`: whether the AUTHORIZED group is meaningful on the platform
