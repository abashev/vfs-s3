# Suite A: File Lifecycle

Prefix: `/file-lifecycle/`

## Steps

```
1. Create file at /file-lifecycle/test-file
   → assert: exists

2. Create file at /file-lifecycle/name with space
   → assert: exists

3. Resolve /file-lifecycle/test-file
   → assert: lastModifiedTime > 0
   → try setLastModifiedTime(111) → assert: throws error

4. Resolve /file-lifecycle/test-file (existing file)
   → try createFolder at same path → assert: throws error

5. Move /file-lifecycle/test-file to /file-lifecycle/renamed
   → assert: /renamed exists, /test-file does not exist
   → move back: /renamed → /test-file
   → assert: /test-file exists, /renamed does not exist
   → try move /test-file to itself → assert: throws error

6. Resolve /file-lifecycle/nonexistent → assert: type is IMAGINARY
   Resolve /file-lifecycle/test-file → assert: type is FILE

7. Resolve /file-lifecycle/does/not/exist → assert: does not exist
```

## Teardown

Delete `/file-lifecycle/` recursively.

## Dependencies

Steps 3–6 depend on step 1 (file must exist). Step 7 is independent.
