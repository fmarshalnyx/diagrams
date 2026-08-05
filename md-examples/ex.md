# Task: Migrate this Maven multi-module repo to CI-friendly `${revision}` versioning with snapshot/release GitLab pipelines

## Context

This is a Maven multi-module project (parent pom + submodules) that deploys jars to
Artifactory for other systems to consume. It has an existing `.gitlab-ci.yml`. I want
to standardize on the following scheme:

- The version is defined **once**, in the parent pom, as `<revision>X.Y.Z-SNAPSHOT</revision>`.
  The checked-in pom always carries a `-SNAPSHOT` revision; CI never commits version changes.
- Every commit to `main` deploys SNAPSHOT artifacts to the Artifactory snapshot repo.
- Releases are triggered by pushing a protected tag `vX.Y.Z`. The tag pipeline builds with
  `-Drevision=X.Y.Z` (overriding the pom) and deploys immutable artifacts to the release repo.
- The tag must be consistent with the pom: tag `v1.4.0` is only valid when the pom revision
  is `1.4.0-SNAPSHOT`; otherwise the release job fails.
- Release builds must fail if any dependency is a SNAPSHOT.

## Step 1 — Inspect before changing

First examine the repo and report what you find before editing:

- Current versioning approach in the parent pom and how submodules reference the parent
  (hardcoded versions? already `${revision}`? versions-maven-plugin? release plugin?).
- Whether the flatten-maven-plugin is already configured, and in what mode.
- Current `.gitlab-ci.yml`: stages, jobs, rules/only-except, caching, how Maven settings and
  Artifactory credentials are wired (settings.xml file in repo? CI variables?), and any jobs
  beyond build/deploy (sonar, security scans, pages, etc.) that must be preserved.
- Existing `distributionManagement` and repository ids.
- Any usage of maven-release-plugin or CI jobs that commit version bumps — these will be removed.
- The current version of the project, so the initial `<revision>` value keeps continuity
  (e.g. if the project is at 2.3.1-SNAPSHOT, keep that; do not reset to 1.0.0).

## Step 2 — Pom changes

1. Parent pom:
   - `<version>${revision}</version>` with `<revision>CURRENT-VERSION-SNAPSHOT</revision>` in
     `<properties>` (preserve the current in-flight version).
   - Add/normalize `flatten-maven-plugin` with `<flattenMode>resolveCiFriendliesOnly</flattenMode>`,
     `<updatePomFile>true</updatePomFile>`, bound to `process-resources`, plus a `flatten.clean`
     execution bound to `clean`.
   - Ensure `distributionManagement` has separate release and snapshot repositories with server
     ids matching the CI settings.xml.
   - Add a `release` profile containing maven-enforcer-plugin with `<requireReleaseDeps/>` and
     `<requireReleaseVersion/>`.
2. Submodules:
   - Parent reference uses `<version>${revision}</version>`; remove any explicit `<version>` on
     the modules themselves.
   - Inter-module dependencies use `<version>${project.version}</version>`.
3. Remove maven-release-plugin configuration and any versions-maven-plugin usage that exists
   solely for version bumping.
4. Add `.flattened-pom.xml` to `.gitignore` if not present.

## Step 3 — Pipeline changes

Modify `.gitlab-ci.yml` (preserving unrelated existing jobs and integrating with existing
stages/caching/settings conventions rather than blindly replacing the file):

1. **Merge request pipelines**: run `mvn verify` only; no deploy.
2. **`main` branch**: a `deploy-snapshot` job that
   - asserts the pom `revision` ends in `-SNAPSHOT` (fail otherwise), then
   - runs `mvn deploy`.
3. **Tag pipelines** (`rules: if: $CI_COMMIT_TAG =~ /^v\d+\.\d+\.\d+$/`): a `deploy-release` job that
   - derives `RELEASE_VERSION="${CI_COMMIT_TAG#v}"`,
   - reads the pom revision via `mvn -q -DforceStdout help:evaluate -Dexpression=revision`,
   - fails with a clear error if `RELEASE_VERSION != revision-with--SNAPSHOT-stripped`,
   - runs `mvn -Drevision="$RELEASE_VERSION" -Prelease deploy`.
4. Keep/extend the existing Maven repo cache and `--batch-mode --errors` CLI options; reuse the
   repo's existing settings.xml/credential mechanism for Artifactory.
5. Ensure no job commits or pushes to the repo.

## Step 4 — Verify

- Run `mvn -q help:evaluate -Dexpression=revision -DforceStdout` and confirm the value.
- Run `mvn clean verify` locally in the container to confirm the build still passes and the
  flattened poms contain a resolved numeric version (grep `.flattened-pom.xml` for `${revision}`
  — it must not appear).
- Lint the `.gitlab-ci.yml` (e.g. via `gitlab-ci-local` if available, or at minimum a YAML parse).
- Summarize every file changed and why.

## Constraints

- Do not change groupIds, artifactIds, or the current version number.
- Do not delete or break existing CI jobs unrelated to versioning/deploy (tests, scans, etc.).
- Do not add version-bump automation; bumping `<revision>` after a release is a deliberate
  human commit.
- Ask before proceeding if the repo deviates significantly from the assumptions above
  (e.g. it is not a multi-module build, or deploys somewhere other than Artifactory).

## Follow-up notes for me (include in your final summary)

- Remind me to protect `v*` tags in GitLab (Settings → Repository → Protected tags).
- Remind me to disable redeploy on the Artifactory release repo and set Max Unique Snapshots
  on the snapshot repo.
- Note the post-release workflow: after tagging `vX.Y.Z`, a human commits a bump of
  `<revision>` to the next `-SNAPSHOT` on main.
