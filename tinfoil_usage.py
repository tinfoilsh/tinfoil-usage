import json
import logging

USAGE_REQUEST_HEADER = b"x-tinfoil-request-usage-metrics"
USAGE_RESPONSE_HEADER = b"x-tinfoil-usage-metrics"
MAX_BUFFER_BYTES = 8 * 1024 * 1024

logger = logging.getLogger("tinfoil_usage")


def _loads(data):
    try:
        return json.loads(data)
    except ValueError:
        return None


def _count(source, *names):
    for name in names:
        value = source.get(name)
        if type(value) is int and value >= 0:
            return value
    return 0


def _header(headers, name):
    for key, value in headers:
        if key.lower() == name:
            return value
    return b""


def _event_data(event):
    for line in event.splitlines():
        if line.startswith(b"data:"):
            return line[5:].strip()
    return b""


def extract_usage(document):
    if not isinstance(document, dict):
        return None
    for candidate in (document.get("response"), document):
        if isinstance(candidate, dict) and isinstance(candidate.get("usage"), dict):
            return candidate["usage"]
    return None


def is_counts_only(document):
    return isinstance(document, dict) and document.get("choices") == [] \
        and isinstance(document.get("usage"), dict)


def format_usage(usage):
    prompt = _count(usage, "prompt_tokens", "input_tokens")
    completion = _count(usage, "completion_tokens", "output_tokens")
    total = _count(usage, "total_tokens") or prompt + completion
    fields = [f"prompt={prompt}", f"completion={completion}", f"total={total}"]
    details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
    if isinstance(details, dict):
        cached = min(prompt, _count(details, "cached_tokens"))
        fields += [f"cached_prompt_tokens={cached}", f"uncached_prompt_tokens={prompt - cached}"]
    return ",".join(fields).encode()


def _asked_for_usage_chunk(request):
    document = _loads(request)
    if not isinstance(document, dict):
        return True
    options = document.get("stream_options")
    return isinstance(options, dict) and bool(options.get("include_usage"))


def _streamed(send, trailer, keep_usage_chunk):
    pending = b""
    usage = None

    async def handle(message):
        nonlocal pending, usage

        more = message.get("more_body", False)
        *events, pending = (pending + message.get("body", b"")).split(b"\n\n")
        if pending and not more:
            events.append(pending)
            pending = b""

        kept = []
        for event in events:
            document = _loads(_event_data(event))
            counts = extract_usage(document)
            if counts:
                usage = counts
            if keep_usage_chunk or not is_counts_only(document):
                kept.append(event + b"\n\n")

        await send({"type": "http.response.body", "body": b"".join(kept), "more_body": more or trailer})
        if trailer and not more:
            headers = [(USAGE_RESPONSE_HEADER, format_usage(usage))] if usage else []
            await send({"type": "http.response.trailers", "headers": headers})

    return handle


def _buffered(held, send):
    body = bytearray()

    async def handle(message):
        nonlocal held

        if held is None:
            await send(message)
            return

        body.extend(message.get("body", b""))
        more = message.get("more_body", False)
        if more and len(body) <= MAX_BUFFER_BYTES:
            return

        usage = None if more else extract_usage(_loads(bytes(body)))
        headers = list(held.get("headers", []))
        if usage:
            headers.append((USAGE_RESPONSE_HEADER, format_usage(usage)))
        await send({**held, "headers": headers})
        held = None

        await send({"type": "http.response.body", "body": bytes(body), "more_body": more})
        body.clear()

    return handle


async def _start_response(message, send, metered, request):
    headers = list(message.get("headers", []))

    if _header(headers, b"content-type").split(b";")[0].strip().lower() != b"text/event-stream":
        if metered:
            return _buffered(message, send)
        await send(message)
        return None

    headers = [(name, value) for name, value in headers if name.lower() != b"content-length"]
    if metered:
        headers.append((b"trailer", USAGE_RESPONSE_HEADER))
    await send({**message, "headers": headers})
    return _streamed(send, metered, _asked_for_usage_chunk(request))


class UsageMetricsMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "POST":
            await self.app(scope, receive, send)
            return

        metered = _header(scope["headers"], USAGE_REQUEST_HEADER).lower() == b"true"
        request = bytearray()
        handle_body = None

        async def wrapped_receive():
            message = await receive()
            if message["type"] == "http.request" and len(request) <= MAX_BUFFER_BYTES:
                request.extend(message.get("body", b""))
            return message

        async def wrapped_send(message):
            nonlocal handle_body
            if message["type"] == "http.response.start":
                handle_body = await _start_response(message, send, metered, bytes(request))
            elif message["type"] == "http.response.body" and handle_body:
                await handle_body(message)
            else:
                await send(message)

        await self.app(scope, wrapped_receive, wrapped_send)


def _finish_httptools(cycle, headers):
    encoded = b"".join(name.lower() + b": " + value + b"\r\n" for name, value in headers)
    cycle.transport.write(b"0\r\n" + encoded + b"\r\n")
    if not cycle.keep_alive:
        cycle.transport.close()


def _finish_h11(cycle, headers):
    import h11

    cycle.transport.write(cycle.conn.send(h11.EndOfMessage(headers=headers)))
    if cycle.conn.our_state is h11.MUST_CLOSE or not cycle.keep_alive:
        cycle.conn.send(h11.ConnectionClosed())
        cycle.transport.close()


def _patch_cycle(cycle_class, finish):
    original = cycle_class.send

    async def send(self, message):
        if message["type"] != "http.response.trailers":
            await original(self, message)
            return
        if self.disconnected:
            return
        if self.response_complete:
            logger.warning("usage trailer dropped: response body already terminated")
            return
        self.response_complete = True
        self.message_event.set()
        finish(self, list(message["headers"]))
        self.on_response()

    cycle_class.send = send


# Install the billing patch on all HTTP handlers
def _install_trailers():
    installed = []
    for name, finish in (("httptools_impl", _finish_httptools), ("h11_impl", _finish_h11)):
        try:
            cycle_class = __import__(f"uvicorn.protocols.http.{name}", fromlist=["x"]).RequestResponseCycle
        except (ImportError, AttributeError):
            continue
        _patch_cycle(cycle_class, finish)
        installed.append(name)

    from uvicorn.protocols.http.auto import AutoHTTPProtocol

    selected = AutoHTTPProtocol.__module__.rsplit(".", 1)[-1]
    if selected not in installed:
        raise RuntimeError(
            f"tinfoil_usage: uvicorn will serve with {selected}, which has no trailer "
            "support; streaming requests would be billed zero tokens"
        )
    return installed


TRAILER_SUPPORT = _install_trailers()
