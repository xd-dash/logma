# Scoped file callbacks

`fileflow` composes Logma channel callbacks with the lightweight Google Drive uploader. Credentials never travel in Pub/Sub payloads. A callback setup registers one or more `gdrive.Uploader` values under opaque `credential_id` values, then every file flow is additionally isolated by `request_id` and `subscriber_id`.

The scope is:

```text
Google credential
└── request
    ├── subscriber A
    └── subscriber B
```

The same Google account can therefore service many independent callback requests and many independent subscribers without sharing local files or channel names.

For a scope such as:

```json
{"credential_id":"personal-drive","request_id":"news-backup-01","subscriber_id":"public-company-feed"}
```

News uses two independent channels:

```text
scope:gdrive:personal-drive:request:news-backup-01:subscriber:public-company-feed:news:write
scope:gdrive:personal-drive:request:news-backup-01:subscriber:public-company-feed:news:upload
```

The write callback appends each News event as one NDJSON line. The upload callback takes a locked snapshot, hashes the complete snapshot, and derives a deterministic Drive upload ID from scope + content hash. Drive stores that ID in `appProperties`. Replaying the same upload without new News data therefore resolves the existing Drive object instead of creating a duplicate. New polling data changes the content hash and produces a new snapshot object.

Stocks/options use a separate pair:

```text
scope:gdrive:personal-drive:request:market-backup-01:subscriber:stocks-options:market:write
scope:gdrive:personal-drive:request:market-backup-01:subscriber:stocks-options:market:upload
```

`MarketFlow` keeps one active writer per scope for the lifetime of the callback setup invocation. The market write callback keeps appending to that active segment. An upload message must include a stable `trigger_id`:

```json
{
  "trigger_id":"close-2026-08-28T20:00:00Z",
  "folder_id":"optional-drive-folder",
  "mime_type":"application/x-ndjson"
}
```

The upload callback rotates first: a new active file becomes the target for subsequent market events, the old file is closed and persisted as a `.pending` upload, then the old file is uploaded. Only after Drive confirms success (or confirms the same upload ID already exists) are the old local segment and its pending marker deleted. A failed upload retains the closed file and marker; replaying the same `trigger_id` resumes that file rather than rotating the current stream again.

Typical setup:

```go
registry := gdrive.NewRegistry()
_ = registry.Register("personal-drive", uploader)

scope := fileflow.Scope{
    CredentialID: "personal-drive",
    RequestID:    "market-backup-01",
    SubscriberID: "stocks-options",
}

market := fileflow.NewMarketFlow("/var/lib/logma/callbacks", registry)
handlers, err := market.Handlers(ctx, scope)
// Add handlers to the Logma ServiceSpec / ControlPlane callback set.
```

Use a different `subscriber_id` when the same Drive credential and request need multiple independently written files. Use a different `request_id` when the same credential starts another logical backup/callback request.

No live Drive test is included here; the OAuth refresh token must be supplied through the deployment secret path before exercising the remote upload API.
