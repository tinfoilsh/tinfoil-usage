# vllm-metered

Upstream vLLM images with `tinfoil_usage.py` as a middleware. The gateway bills
traffic it cannot read; `X-Tinfoil-Usage-Metrics` headers are added to ensure
EHBP directly to the client can work.

## Using it

A config-only repo changes `image:` in `tinfoil-config.yml`:

```yaml
image: "ghcr.io/tinfoilsh/vllm-metered:v0.22.0@sha256:<digest>"
```

A repo with a Dockerfile changes its base and drops any local copy of the
module:

```dockerfile
ARG VLLM_BASE_IMAGE=ghcr.io/tinfoilsh/vllm-metered:v0.27.1@sha256:<digest>
```

Either way the repo also needs three entries in the container's `command:`,
which cannot move into the image — `command:` overrides the image's `CMD`
outright:

```yaml
"--enable-force-include-usage",
"--middleware",
"tinfoil_usage.UsageMetricsMiddleware",
"--enable-prompt-tokens-details",
```

