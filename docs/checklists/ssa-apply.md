# T3.3.6 – SSA Apply Checkpoint

This checkpoint records the commands and artifacts used to verify the new
server-side apply (SSA) path for Bundles.

## Test Execution

```
cargo test apply_conflicts_without_force apply_dry_run_does_not_persist_mutations
```

- `apply_conflicts_without_force` asserts that the registry surfaces HTTP 409
  conflicts when the CLI touches `/spec/options` without `--force`.
- `apply_dry_run_does_not_persist_mutations` validates that `--dry-run`
  returns the speculative spec/owners without persisting them.

## Conflict Artifact

Sample response captured from the API during the conflict test:

```json
{
  "error": "Apply would modify fields managed by another actor",
  "conflicts": [
    {
      "path": "/spec/options",
      "existingManager": "controller/bundle"
    }
  ]
}
```

## CLI Dry-run Output

```
$ nanocloud bundle apply demo -n default -f patch.yaml \
    --field-manager cli.nanocloud/v1 --dry-run
Dry-run succeeded for 'default/demo' (resourceVersion would become 6821).
```

The CLI response confirms that SSA can be validated in isolation before handing
ownership to a new field manager.
