# Google Drive callback

This callback uploads local files to personal Google Drive accounts without the Google API SDK. OAuth refresh and Drive API calls use only the Go standard library.

A `gdrive.Registry` maps an opaque `credential_id` to an `Uploader`. Pub/Sub messages carry only that ID through the higher-level `fileflow.Scope`; client IDs, client secrets, access tokens, and refresh tokens never belong in channel names or payloads.

For one credential, `GOOGLE_DRIVE_CREDENTIALS_FILE` and `GOOGLE_DRIVE_REFRESH_TOKEN` remain supported by `NewFromEnv()`. For multiple personal Drive identities, construct one uploader per secret set and register each under a separate credential ID.

The refresh token must have been minted with a Drive scope capable of creating and finding files created by this app, such as `https://www.googleapis.com/auth/drive.file`.

`UploadRequest.UploadID` provides remote idempotency. When set, Logma stores it in Drive file `appProperties.logma_upload_id` and queries Drive before creating a file. If the object is already present, `Upload` returns it with `AlreadyExists=true` rather than creating a duplicate. The scoped News and market callbacks generate these IDs automatically.

Low-level payload example:

```json
{
  "path": "/var/lib/logma/export/events.ndjson",
  "name": "events.ndjson",
  "folder_id": "optional-drive-folder-id",
  "mime_type": "application/x-ndjson",
  "upload_id": "stable-idempotency-key"
}
```

For application flows, prefer `serverless/callbacks/fileflow`: it separates News write/upload channels and market write/rotate-upload channels and scopes them by Drive credential, request, and subscriber.

A small standalone binary remains available at `./cmd/logma-gdrive-callback` for one credential loaded from environment variables.

No live Drive integration test should be run until a real refresh token is supplied through secrets.
