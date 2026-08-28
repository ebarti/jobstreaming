# Releasing JobStreaming

Releases are built and published only by
[`release.yml`](.github/workflows/release.yml) after a `v*` tag is pushed. The
workflow does not use a manually stored PyPI API token.

## Prepare the release commit

Before creating a tag:

1. Confirm the release commit is merged into `main` and its CI checks pass.
2. Confirm `pyproject.toml`, the package version, and the changelog entry agree.
3. Replace `Unreleased` on the matching changelog heading with the release date.
4. Run the development checks from [CONTRIBUTING.md](CONTRIBUTING.md), build both
   distributions, and verify them with:

   ```bash
   python scripts/verify_release_artifacts.py \
     --expected-version 0.0.4 \
     --dist-dir dist
   ```

5. Review the release diff for credentials, authorization material, personal data,
   and sensitive board-response content.
6. Confirm the repository's `pypi` environment and the PyPI trusted-publisher mapping
   still target `.github/workflows/release.yml`. This is an external configuration
   check; do not add publisher credentials to the repository.

Create `v0.0.4` only after those checks are complete. Do not move or reuse a published
tag.

## Automated trust chain

The tag workflow:

1. verifies that the tagged commit belongs to `main` and the tag matches the package
   version;
2. tests the locked project and builds one wheel and one source distribution;
3. installs each distribution in a clean environment outside the checkout, runs
   `pip check`, validates installed metadata, and exercises the public streaming and
   SDK surfaces;
4. verifies the default wheel without Pandas, strict consumer typing, and a separate
   installation of the `batch` extra;
5. verifies the source distribution's public streaming and SDK surfaces;
6. records SHA-256 checksums and uploads the release payload as one immutable workflow
   artifact;
7. generates GitHub-hosted SLSA build provenance in a separate job with narrowly
   scoped OIDC and attestation permissions;
8. publishes through the dedicated `pypi` environment using Trusted Publishing, with
   PyPI attestations enabled; and
9. creates the GitHub release only after PyPI publication succeeds.

The build job has no OIDC permission. The attestation and PyPI jobs receive OIDC only
after the build artifact has been produced.

## Verify a published release

Download the release assets and verify their checksums:

```bash
gh release download v0.0.4 \
  --repo ebarti/jobstreaming \
  --pattern "jobstreaming-*" \
  --pattern "SHA256SUMS"
sha256sum --check SHA256SUMS
```

On macOS, use `shasum -a 256 --check SHA256SUMS` for the checksum step.

Then verify GitHub provenance for both distributions:

```bash
gh attestation verify jobstreaming-0.0.4-py3-none-any.whl \
  --repo ebarti/jobstreaming
gh attestation verify jobstreaming-0.0.4.tar.gz \
  --repo ebarti/jobstreaming
```

Finally, install the wheel into a fresh environment and confirm dependency integrity:

```bash
python -m venv /tmp/jobstreaming-release-check
/tmp/jobstreaming-release-check/bin/python -m pip install \
  jobstreaming-0.0.4-py3-none-any.whl
/tmp/jobstreaming-release-check/bin/python -m pip check
/tmp/jobstreaming-release-check/bin/python -c \
  'from importlib.metadata import version; assert version("jobstreaming") == "0.0.4"'
```

PyPI and the GitHub release should contain the same wheel and source-distribution
digests recorded in `SHA256SUMS`.
