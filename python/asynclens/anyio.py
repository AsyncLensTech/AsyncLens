import logging
import threading
import anyio
from asynclens import _native

# Default frames to ignore when parsing the call stack to focus strictly on user code
DEFAULT_IGNORE_PATTERNS = ["site-packages", "lib/python", "<string>", "<frozen importlib", "multiprocessing"]

class AnyIOAsyncWatchdog:
    """
    AnyIO-compatible Event Loop Watchdog daemon.
    Operates agnostically across standard asyncio or high-performance trio backends.
    """
    def __init__(
        self, 
        threshold_ms: int = 50, 
        logger: logging.Logger | None = None, 
        name: str | None = None,
        ignore_patterns: list[str] | None = None,
        tg: anyio.abc.TaskGroup | None = None
    ):
        # Fallback to the default package logger if none is injected
        self.logger = logger if logger is not None else logging.getLogger("asynclens")
        self.threshold_ms = threshold_ms       
        
        # Grab the current OS thread name if a custom alias isn't explicitly provided
        self.name = name if name is not None else threading.current_thread().name
        self.tg = tg
        
        # Initialize default ignore patterns if the user didn't specify custom exclusions
        if ignore_patterns is None:
            ignore_patterns = DEFAULT_IGNORE_PATTERNS
            
        # Instantiate the low-level native Rust watchdog module
        self.watchdog = _native.AsyncWatchdog(
            threshold_ms=self.threshold_ms, 
            logger=self.logger,
            ignore_patterns=ignore_patterns
        )
        self.cancel_scope = None

    async def start(self, tg: anyio.abc.TaskGroup | None = None):
        """
        Starts the background native Rust thread and registers the asynchronous 
        heartbeat pinger loop inside the active AnyIO TaskGroup.
        """
        task_group = tg if tg is not None else self.tg
        if task_group is None:
            raise RuntimeError("An active AnyIO TaskGroup (tg) must be passed either via __init__ or start().")

        # Notify the native Rust core about the current thread identity
        self.watchdog.register_thread(self.name)
        
        async def anyio_loop_pinger():
            # Encapsulate the heartbeat task within an AnyIO CancelScope for safe cancellation mechanics
            with anyio.CancelScope() as scope:
                self.cancel_scope = scope
                while True:
                    # Poke the atomic witness boolean inside the Rust extension
                    self.watchdog.heartbeat()
                    # Relinquish execution control back to the loop for 10ms
                    await anyio.sleep(0.01)

        # Launch the underlying native C/Rust tracking daemon
        self.watchdog.start()
        
        # Non-blocking injection of the pinger loop into the application task group
        task_group.start_soon(anyio_loop_pinger)

    async def stop(self):
        """
        Gracefully tears down the asynchronous pinger task via AnyIO's CancelScope
        and requests the Rust tracking thread to spin down safely.
        """
        if self.cancel_scope:
            self.cancel_scope.cancel()
        
        # Instruct the native backend to destroy or join its tracking loops
        self.watchdog.stop()
        
        # Yield execution momentarily to guarantee the Rust daemon completes its cleanup operations
        await anyio.sleep(self.threshold_ms / 1000.0 + 0.05)

    async def __aenter__(self):
        """
        Asynchronous context manager support (`async with`).
        Requires that the instance was initialized with an existing `tg` reference.
        """
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures the daemon detaches cleanly when exiting the asynchronous context.
        """
        await self.stop()
        return False
