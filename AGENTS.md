# Agent guidance for Resolve Artist Relations

## Repository scope

This directory git repository for the personal MusicBrainz
Picard plugin "Resolve Artist Relations". Keep all plugin source, manifest,
tests, and plugin documentation changes in this repository.

The parent directory is a separate Git repository containing the NixOS devenv,
and `../picard/` is a separate upstream Picard clone. Do not add parent or
upstream files to this repository. Do not copy this plugin into the Picard
source tree; Picard's local-development mode loads this working tree directly.

Use explicit Git scopes when a task crosses repository boundaries:

```console
git -C .. status --short --branch
git -C ../picard status --short --branch
git -C . status --short --branch
```

The current Picard v3 migration work is on branch `v3_migration`. Do not switch
branches, rewrite history, commit, or push unless the user requests it.

## Picard v3 plugin contract

This is a manifest-based Picard v3 plugin:

- `MANIFEST.toml` contains its stable UUID and public metadata.
- `__init__.py` provides `enable(api: PluginApi)`.
- Registration must go through the `PluginApi` instance supplied to `enable`.
- Runtime UI imports use PyQt6, not PyQt5.
- The declared Picard plugin API is `3.0`.

Preserve UUID `684e2181-d43e-4b23-940e-eb10d26ed9f4`. Changing it makes Picard
treat the plugin as a different plugin and disconnects saved plugin state.

The release workflow replaces the patch component of the manifest version.
Understand that workflow before changing version-generation behavior.

## Purpose and behavior

The plugin registers a track metadata processor. For every track it resolves
MusicBrainz artist details and selected artist-to-artist relations, serializes
the result as JSON, and stores it in the metadata field:

```text
artist_relations_json
```

Artist requests are shared through `WebrequestQueue` and cached across
resolver instances. Album tasks are used to keep Picard's asynchronous loading
state correct while requests are active. Changes to this area must preserve:

- one completion for every album task added;
- completion on success and error paths;
- correct behavior when several tracks share an artist request;
- no UI access or blocking network work outside Picard's supported APIs;
- the MusicBrainz request-rate constraint;
- recursion termination and the configured traversal-depth limit;
- valid JSON output for multi-artist tracks and nested relationships.

Prefer `api.logger`, `api.web_service`, task APIs, and registration methods on
the supplied `PluginApi`. Avoid reaching into Picard internals when the v3 API
provides an equivalent operation.

## Development environment

Run development commands from the parent workspace inside devenv:

```console
cd ..
devenv shell
```

The parent environment deliberately isolates Picard settings, cache, plugins,
data, state, and runtime sockets beneath `../.picard-dev/`. Never bypass these
overrides by launching the user's system-installed Picard for plugin testing.

Validate the manifest with the development Picard checkout:

```console
picard-plugin-validate
```

Equivalent command from the parent directory:

```console
picard-cli plugins validate artist-resolver-musicbrainz-picard-plugin/
```

Register the working tree in local-development mode:

```console
picard-plugin-install
```

Launch Picard with debug logging:

```console
picard-dev --debug
```

After registration, Picard loads code directly from this repository. Restart
Picard after code changes when a full reload is needed. Reinstall only after
the isolated profile was reset, the plugin was unregistered, or installation
metadata changed in a way that requires it.

## Verification expectations

Manifest validation checks structure and compatibility, but it does not prove
runtime behavior. For manifest-only changes, at minimum run:

```console
picard-plugin-validate
```

For Python changes, also:

1. Start `picard-dev --debug` and confirm the plugin loads without a traceback.
2. Load a representative MusicBrainz release containing solo and multi-artist
   tracks, including at least one traversed relationship type.
3. Confirm album loading finishes rather than remaining blocked.
4. Inspect `artist_relations_json` and verify it is valid JSON with expected
   artist IDs, join phrases, aliases, and nested relations.
5. Exercise a failed or timed-out request when changing queue or task logic.

There is currently no automated test suite in this repository. When fixing a
bug or changing nontrivial queue, recursion, serialization, or response logic,
add focused tests when feasible rather than relying only on manual GUI checks.

## Code changes

Follow modern Python conventions and Picard's upstream style for new code:

- four-space indentation;
- imports grouped at the top of the module;
- `snake_case` names for functions and variables;
- clear type hints for new or substantially changed interfaces;
- direct boolean expressions instead of comparisons such as `False == value`;
- no broad drive-by reformatting mixed into behavioral changes.

The migrated file still contains older naming and indentation. Improve nearby
code when it helps the requested change, but keep unrelated cleanup separate so
behavioral reviews remain understandable.

Before handoff, run `git diff --check`, inspect this repository's status, and
report any runtime paths that were not exercised.
