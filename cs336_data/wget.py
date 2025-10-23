#!/usr/bin/env python3
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from io import BytesIO
from rich import print
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
)
import asyncio
import aiohttp
from typing import Optional, Sequence, Tuple
from dataclasses import dataclass
import time

URL_LIST_FILE = "subsampled_positive_urls.txt"
WARC_OUTPUT = "subsampled_positive_urls.warc"
TIMEOUT_SECONDS = 10
TOTAL_CONCURRENT_REQUESTS = 32
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


@dataclass
class FetchResult:
    url: str
    status: Optional[int] = None
    reason: Optional[str] = None
    headers: Optional[Sequence[Tuple[str, str]]] = None
    content: bytes = b""
    error: Optional[Exception] = None
    retries: int = 0


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout,
    max_retries: int = MAX_RETRIES,
) -> FetchResult:
    """Fetch URL with retry logic for transient failures."""
    last_error = None

    for attempt in range(max_retries):
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
                    retries=attempt,
                )
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            ConnectionError,
        ) as exc:
            last_error = exc
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                delay = RETRY_DELAY * (2**attempt)
                await asyncio.sleep(delay)
            continue
        except Exception as exc:
            # Non-retryable errors (e.g., invalid URL)
            return FetchResult(url=url, error=exc, retries=attempt)

    return FetchResult(url=url, error=last_error, retries=max_retries)


def write_record(
    writer: WARCWriter,
    result: FetchResult,
) -> bool:
    """Write a fetch result to WARC file. Returns True if successful."""
    if result is None:
        return False

    if result.error:
        print(f"[red]Failed to fetch {result.url}: {result.error}[/red]")
        return False

    status_code = result.status or 0
    reason = result.reason or ""
    if status_code != 200:
        print(
            f"[yellow]Warning: Non-200 status for {result.url}: {status_code} {reason}[/yellow]"
        )
        return False

    status_line = f"{status_code} {reason}".strip()
    http_headers = StatusAndHeaders(
        status_line,
        headers=list(result.headers or []),
        protocol="HTTP/1.1",
    )

    record = writer.create_warc_record(
        result.url,
        record_type="response",
        payload=BytesIO(result.content),
        http_headers=http_headers,
    )
    writer.write_record(record)
    return True


async def main(input_file, output_file):
    """Main function with improved task management and progress tracking."""
    # Count total URLs
    total_line_count = 0
    with open(input_file, "r", encoding="utf-8") as url_file:
        total_line_count = sum(1 for _ in url_file)

    # Statistics
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "non_200": 0,
    }

    # Configure connector with proper limits
    connector = aiohttp.TCPConnector(
        limit=100,  # Total connection pool size
        limit_per_host=10,  # Limit per host to avoid overwhelming servers
        ttl_dns_cache=300,  # DNS cache TTL
        enable_cleanup_closed=True,
    )

    # Set up client session with proper timeout and headers
    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT_SECONDS,
        connect=5,  # Connection timeout
        sock_read=5,  # Socket read timeout
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CS336Bot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    with (
        open(input_file, "r", encoding="utf-8") as url_file,
        open(output_file, "wb") as warc_stream,
        Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            TextColumn("• {task.fields[status]}"),
        ) as progress,
    ):
        writer = WARCWriter(warc_stream, gzip=True)
        task_id = progress.add_task(
            "Fetching URLs",
            total=total_line_count,
            status="Starting...",
        )

        pending_tasks = set()

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as session:
            for raw_line in url_file:
                url = raw_line.strip()
                if not url or url.startswith("#"):
                    continue

                stats["total"] += 1

                # Create fetch task
                task = asyncio.create_task(fetch(session, url, timeout))
                pending_tasks.add(task)

                # When we reach concurrency limit, wait for one to complete
                if len(pending_tasks) >= TOTAL_CONCURRENT_REQUESTS:
                    done, pending_tasks = await asyncio.wait(
                        pending_tasks, return_when=asyncio.FIRST_COMPLETED
                    )

                    # Process completed tasks
                    for completed_task in done:
                        result = await completed_task
                        if write_record(writer, result):
                            stats["success"] += 1
                        elif result.error:
                            stats["failed"] += 1
                        else:
                            stats["non_200"] += 1

                        # Update progress
                        progress.update(
                            task_id,
                            advance=1,
                            status=f"✓ {stats['success']} | ✗ {stats['failed']} | ⚠ {stats['non_200']}",
                        )

            # Process remaining tasks
            if pending_tasks:
                done, _ = await asyncio.wait(pending_tasks)
                for completed_task in done:
                    result = await completed_task
                    if write_record(writer, result):
                        stats["success"] += 1
                    elif result.error:
                        stats["failed"] += 1
                    else:
                        stats["non_200"] += 1

                    progress.update(
                        task_id,
                        advance=1,
                        status=f"✓ {stats['success']} | ✗ {stats['failed']} | ⚠ {stats['non_200']}",
                    )

    # Print final statistics
    print("\n[bold green]Fetch Complete![/bold green]")
    print(f"Total URLs processed: {stats['total']}")
    print(
        f"Successfully archived: {stats['success']} ([green]{stats['success']/stats['total']*100:.1f}%[/green])"
    )
    print(
        f"Failed (errors): {stats['failed']} ([red]{stats['failed']/stats['total']*100:.1f}%[/red])"
    )
    print(
        f"Non-200 status: {stats['non_200']} ([yellow]{stats['non_200']/stats['total']*100:.1f}%[/yellow])"
    )


if __name__ == "__main__":
    import sys

    input_file = sys.argv[1] if len(sys.argv) > 1 else URL_LIST_FILE
    output_file = sys.argv[2] if len(sys.argv) > 2 else WARC_OUTPUT
    print(f"Reading URLs from: {input_file}")
    print(f"Writing WARC to: {output_file}")
    asyncio.run(main(input_file, output_file))
