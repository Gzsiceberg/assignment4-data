#!/usr/bin/env python3
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from io import BytesIO
from rich import print
from rich.progress import track
import asyncio
import aiohttp
from typing import Optional, Sequence, Tuple
from dataclasses import dataclass

URL_LIST_FILE = "subsampled_positive_urls.txt"
WARC_OUTPUT = "subsampled_positive_urls.warc"
TIMEOUT_SECONDS = 5
TOTAL_CONCURRENT_REQUESTS = 32


@dataclass
class FetchResult:
    url: str
    status: Optional[int] = None
    reason: Optional[str] = None
    headers: Optional[Sequence[Tuple[str, str]]] = None
    content: bytes = b""
    error: Optional[Exception] = None


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout,
) -> FetchResult:
    try:
        async with session.get(url, timeout=timeout) as response:
            content = await response.read()
            headers = [(k, v) for k, v in response.headers.items()]
            return FetchResult(
                url=url,
                status=response.status,
                reason=response.reason,
                headers=headers,
                content=content,
            )
    except Exception as exc:
        return FetchResult(url=url, error=exc)
    

def write_record(
    writer: WARCWriter,
    result: FetchResult,
):
    if result is None:
        return

    if result.error:
        print(f"[red]Failed to fetch {result.url}: {result.error}[/red]")
        return
    
    status_code = result.status or 0
    reason = result.reason or ""
    if status_code != 200:
        print(f"[yellow]Warning: Non-200 status for {result.url}: {status_code} {reason}[/yellow]")
        return

    status_line = f"{status_code} {reason}".strip()
    http_headers = StatusAndHeaders(
        status_line,
        headers=list(result.headers or []),
        protocol="HTTP/1.1",
    )

    resp_record = writer.create_warc_record(
        result.url,
        record_type="response",
        payload=BytesIO(result.content),
        http_headers=http_headers,
    )
    writer.write_record(resp_record)


async def main(input_file, output_file):
    total_line_count = 0
    with open(input_file, "r", encoding="utf-8") as url_file:
        total_line_count = sum(1 for _ in url_file)

    connector = aiohttp.TCPConnector(limit=100)
    with (
        open(input_file, "r", encoding="utf-8") as url_file,
        open(output_file, "wb") as warc_stream,
    ):
        writer = WARCWriter(warc_stream, gzip=False)
        all_tasks = []
        async with aiohttp.ClientSession(connector=connector) as session:
            for raw_line in track(
                url_file, description="Processing URLs", total=total_line_count
            ):
                url = raw_line.strip()
                if not url or url.startswith("#"):
                    continue
                task = asyncio.create_task(fetch(
                    session,
                    url,
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                ))
                all_tasks.append(task)

                if len(all_tasks) < TOTAL_CONCURRENT_REQUESTS:
                    continue

                first_done = await anext(asyncio.as_completed(all_tasks))
                result = await first_done
                print(f"[OK] Archived {result.url}")
                write_record(writer, result)
                all_tasks.remove(first_done)


if __name__ == "__main__":
    import sys

    input_file = sys.argv[1] if len(sys.argv) > 1 else URL_LIST_FILE
    output_file = sys.argv[2] if len(sys.argv) > 2 else WARC_OUTPUT
    print(f"Reading URLs from: {input_file}")
    print(f"Writing WARC to: {output_file}")
    asyncio.run(main(input_file, output_file))
