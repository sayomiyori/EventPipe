import asyncio
from typing import Final

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


_OK: Final = b"OK"


def _http_response(status: str, content_type: str, body: bytes) -> bytes:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    return ("\r\n".join(headers)).encode("utf-8") + body


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        data = await reader.read(4096)
        line = data.split(b"\r\n", 1)[0] if data else b""
        parts = line.split()
        path = parts[1].decode("utf-8", errors="ignore") if len(parts) >= 2 else "/"

        if path.startswith("/health"):
            resp = _http_response("200 OK", "text/plain", _OK)
        elif path.startswith("/metrics"):
            payload = generate_latest()
            resp = _http_response("200 OK", CONTENT_TYPE_LATEST, payload)
        else:
            resp = _http_response("404 Not Found", "text/plain", b"not found")
        writer.write(resp)
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def start_health_server(host: str, port: int) -> asyncio.AbstractServer:
    return await asyncio.start_server(_handle, host=host, port=port)
