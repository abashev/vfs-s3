# ADR-003: GitHub Actions Supply Chain Security

## Status

Accepted

## Context

In March 2025, a supply chain attack compromised `tj-actions/changed-files` (CVE-2025-30066),
affecting 23,000+ repositories. Attackers force-pushed malicious code to existing version tags,
causing CI runners to execute compromised code that leaked secrets from workflow logs. A similar
attack hit `aquasecurity/trivy-action` in March 2026, compromising 75 of 76 version tags.

GitHub Actions references like `uses: actions/checkout@v6` resolve to a mutable tag. An attacker
who gains push access to a repository can update a tag to point at a malicious commit. Since tags
are mutable Git references, this is a fundamental risk in the GitHub Actions model.

Our project uses 11 third-party actions across 6 workflows, making this a real attack surface.

## Decision

We adopt the following measures to harden our CI/CD pipeline:

### 1. Pin all actions to commit SHAs

Every `uses:` reference must specify a full 40-character commit SHA with a version comment:

```yaml
- uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6
```

Commit SHAs are immutable — even if a tag is hijacked, the pinned SHA cannot change.

### 2. Enforce pinning with a CI check

A dedicated workflow (`sha-pinning-check.yml`) runs
`zgosalvez/github-actions-ensure-sha-pinned-actions` on every PR and push. It scans all
workflow files and fails if any action is referenced by tag or branch instead of SHA.

### 3. Minimal workflow permissions

Every workflow explicitly declares the minimum `permissions` it needs. The default token
permission should be set to read-only at the repository level (Settings → Actions → General →
Workflow permissions).

### 4. Dependabot for actions updates

Dependabot is configured with `package-ecosystem: "github-actions"` to automatically create
PRs when pinned actions have new versions. This keeps SHAs current while maintaining the
security of pinned references.

## Consequences

**Positive:**

- Immune to tag hijacking attacks (tj-actions, trivy-action class)
- CI check prevents accidental introduction of unpinned actions
- Version comments maintain readability

**Negative:**

- SHA references are less readable than version tags (mitigated by comments)
- Updating actions requires resolving new SHAs (mitigated by Dependabot)
- Need to verify that Dependabot-proposed SHA updates point to legitimate releases

## References

- [CVE-2025-30066 — tj-actions/changed-files](https://github.com/advisories/ghsa-mrrh-fwg8-r2c3)
- [Trivy GitHub Actions compromise (2026)](https://snyk.io/articles/trivy-github-actions-supply-chain-compromise/)
- [CISA advisory](https://www.cisa.gov/news-events/alerts/2025/03/18/supply-chain-compromise-third-party-github-action-cve-2025-30066)
- [Ensure SHA-pinned actions](https://github.com/zgosalvez/github-actions-ensure-sha-pinned-actions)
