# AsyncLens ⚡

An ultra-performant, low-overhead native event loop watchdog for Python **Asyncio** and **AnyIO** applications, powered by a fast **Rust core**. 

**AsyncLens** detects silent event loop starvation in production environments—such as when a misplaced synchronous blocking call (`time.sleep()`, heavy mathematical computations, or a non-async database driver) paralyzes your execution thread—and isolates the exact offending lines of code instantly.

## Why AsyncLens?

- **Zero-Cost Abstraction:** Built on PyO3 and native Rust atomic synchronization primitives (`AtomicBool` with `Ordering::Relaxed`).
- **No GIL Contention:** The monitoring watchdog executes on an isolated native OS thread entirely outside the Python execution path, preserving microsecond-level runtime performance.
- **Production Deduplication:** Implements a deterministic state machine to group cascading thread freezes into a single, clean timeline block. It fires a single alert when the loop stalls and a single recovery update when traffic resumes—eliminating alert fatigue and log storming.
- **Lean Architecture:** Distributed with zero unnecessary dependencies. Standard `asyncio` loop handling is fully self-contained, while `anyio` support lives in a separate, isolated import module.
- **Framework Agnostic:** Seamlessly integrates with raw async scripts, background task workers (`Celery`, `Arq`), or modern web frameworks like **FastAPI** and **Starlette** via standard lifecycle context managers.

## Getting started

Install AsyncLens with `pip install asynclens` or `uv add asynclens`

### Quick example with FastAPI
Copy and paste the following code in a file named `api.py`

```
import asyncio
import logging
import time

from contextlib import asynccontextmanager

from asynclens import AsyncWatchdog
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncWatchdog(threshold_ms=50, logger=logger):
        yield

app = FastAPI(lifespan=lifespan)

@app.get("/good-async")
async def good():
    await asyncio.sleep(0.5)
    return {"ok": "done"}


@app.get("/bad-async")
async def slow():
    time.sleep(0.5)
    return {"ok": "done"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", reload=False, workers=4)

```


### 🚀 Need Distributed Production Alerts?
The core engine runs entirely locally inside your application logs. If you need real-time **Slack/Discord notifications**, cloud ingestion pipelines, or a **centralized performance dashboard** for multi-worker production clusters, [Join the AsyncLens Pro Waitlist ↗](https://forms.gle/pnBG3HrRtrRwzfYG9).

