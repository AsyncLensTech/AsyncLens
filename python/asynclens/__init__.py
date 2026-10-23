import asyncio
import logging
import threading
from asynclens import _native

DEFAULT_IGNORE_PATTERNS = ["site-packages", "lib/python", "<string>", "<frozen importlib", "multiprocessing"]

class AsyncWatchdog:
    def __init__(
        self, 
        threshold_ms: int = 50, 
        loop=None, 
        logger: logging.Logger | None = None, 
        name: str | None = None,
        ignore_patterns: list[str] | None = None # <-- Ajout du paramètre
    ):
        self.logger = logger if logger is not None else logging.getLogger("asynclens")
        self.loop = loop if loop is not None else asyncio.get_running_loop()
        self.threshold_ms = threshold_ms       
        self.name = name if name is not None else threading.current_thread().name
        
        # Initialisation des motifs par défaut si aucun n'est fourni
        if ignore_patterns is None:
            ignore_patterns = DEFAULT_IGNORE_PATTERNS
            
        # Transmission des motifs au constructeur natif Rust
        self.watchdog = _native.AsyncWatchdog(
            threshold_ms=self.threshold_ms, 
            logger=self.logger,
            ignore_patterns=ignore_patterns
        )
        self.pinger_task = None

    def start(self):
        self.watchdog.register_thread(self.name)
        
        async def loop_pinger():
            while True:
                self.watchdog.heartbeat()
                await asyncio.sleep(0.01)
                
        self.watchdog.start()
        self.pinger_task = self.loop.create_task(loop_pinger())

    async def stop(self):
        if self.pinger_task and not self.pinger_task.done():
            self.pinger_task.cancel()
        self.watchdog.stop()
        await asyncio.sleep(self.threshold_ms / 1000.0 + 0.05)

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False
