# Google Drive callback

This callback uploads a local file to a personal Google Drive account without the Google API SDK. It uses only the Go standard library: the OAuth refresh-token exchange and Drive multipart upload are plain HTTPS requests.

Required environment:

- `GOOGLE_DRIVE_CREDENTIALS_FILE`: path to a Google OAuth client JSON file. Desktop (`installed`), web (`web`), or flat `client_id` / `client_secret` JSON is accepted.
- `GOOGLE_DRIVE_REFRESH_TOKEN`: personal-account refresh token. Keep this in GitHub Actions secrets or another secret store; do not commit it.

The refresh token must have been minted after granting a Drive scope capable of creating files, such as `https://www.googleapis.com/auth/drive.file`.

Payload:

```json
{
  "path": "/var/lib/logma/export/events.jsonl",
  "name": "events.jsonl",
  "folder_id": "optional-drive-folder-id",
  "mime_type": "application/x-ndjson"
}
```

The reusable Go API is `gdrive.NewFromEnv()` plus `Uploader.Handle(ctx, payload)` or `Uploader.Upload(ctx, request)`. A small standalone binary is available at `./cmd/logma-gdrive-callback`; it accepts one JSON payload as command-line text or one line on stdin.

No live Drive integration test should be run until a real refresh token is supplied through secrets.
