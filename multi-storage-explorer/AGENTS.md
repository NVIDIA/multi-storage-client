# Multi-Storage Explorer (JavaScript)

Web UI for browsing and managing objects across cloud storage backends. The frontend is bundled into the multi-storage-client Python package; the backend is a Python server in `multi-storage-client/src/multistorageclient/explorer/`. See the root `AGENTS.md` for overall architecture.

## Structure

```text
src/
  App.jsx                 Root component
  main.jsx                Entry point
  components/
    ConfigUploader.jsx    MSC config file upload
    FileManager.jsx       File/object browser
    FilePreview.jsx       Object content preview
  services/
    api.js                Backend API client (axios)
  __tests__/              Unit tests (co-located mirror of src/)
    setup.js              Test setup (jsdom)
index.html                HTML entry point
vite.config.js            Vite build config
vitest.config.js          Vitest test config
eslint.config.js          ESLint config
```

## Commands

| Task | Command |
|---|---|
| Install dependencies | `cd multi-storage-explorer && just prepare-toolchain` |
| Start frontend dev server | `cd multi-storage-explorer && just start-frontend` |
| Start backend server | `cd multi-storage-explorer && just start-backend` |
| Lint & format | `cd multi-storage-explorer && just analyze` |
| Run unit tests | `cd multi-storage-explorer && just run-unit-tests` |
| Build frontend bundle | `cd multi-storage-explorer && just bundle` |
| Full release build | `just multi-storage-explorer/build` |

## Testing

- Tests mirror the source structure under `src/__tests__/`.
- Uses vitest with jsdom and React Testing Library.
- Run with `just run-unit-tests` from this directory.

## Tooling

- **Runtime:** bun
- **Framework:** React 19, Ant Design 6
- **Bundler:** Vite
- **Test runner:** vitest (with coverage via @vitest/coverage-v8)
- **Linter:** ESLint

## Conventions

- The frontend bundle output goes to `multi-storage-client/src/multistorageclient/explorer/static/` (gitignored, included in the Python wheel).
- The backend is part of multi-storage-client, not this sub-project. Start it with `just start-backend` which delegates to the MSC explorer command.

## Boundaries

- **Always:** run `just analyze` and `just run-unit-tests` before submitting changes.
- **Ask first:** changes to the API contract in `services/api.js` (must stay in sync with the Python backend).
- **Never:** commit the `node_modules/` directory or the bundled output in `multi-storage-client/src/multistorageclient/explorer/static/`.
