# tinfoil-usage

`tinfoil_usage.py` is a vLLM middleware that reports per-request token counts
to the gateway. The gateway bills traffic it cannot read, so the inference
server is the only party that can count tokens. It reports them on
`X-Tinfoil-Usage-Metrics`: a response header when they are known up front, an
HTTP trailer when the stream has to end first.

This repo is just the module. Model repos fetch it at build time.

## Using it

In the model repo's Dockerfile, pinned to a commit and a checksum:

```dockerfile
ADD --checksum=sha256:<sha256 of the file> \
    https://raw.githubusercontent.com/tinfoilsh/vllm-metered/<commit>/tinfoil_usage.py \
    /opt/tinfoil/tinfoil_usage.py
ENV PYTHONPATH=/opt/tinfoil
RUN python3 -B -c "import tinfoil_usage; print('usage metering ready:', tinfoil_usage.TRAILER_SUPPORT)"
```

Needs `# syntax=docker/dockerfile:1.5` or later for `--checksum`, which the
model repos already have. A tampered or truncated fetch fails the build rather
than shipping. `/opt/tinfoil` rather than dist-packages: the bases span several
Python versions, so the site-packages path is not the same in all of them. The
`RUN` fails the build if the module cannot patch uvicorn, rather than leaving
it to be found on the first billed request.

Get the checksum with `sha256sum tinfoil_usage.py`.

Then three entries in the container's `command:` in `tinfoil-config.yml`, which
cannot move into the image — `command:` overrides the image's `CMD` outright:

```yaml
"--enable-force-include-usage",
"--middleware",
"tinfoil_usage.UsageMetricsMiddleware",
"--enable-prompt-tokens-details",
```

`--enable-force-include-usage` is what makes the counts exist on a stream at
all; callers that did not ask for that chunk never see it, because the
middleware drops it back out. `--enable-prompt-tokens-details` populates the
cached-token count.
