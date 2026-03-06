import platform
from pathlib import Path
from typing import Optional, Dict, Any, List
from playwright.sync_api import sync_playwright, BrowserContext

from src.core.requester import Requester


def get_default_chrome_user_data_dir() -> str:
    system = platform.system()
    home = Path.home()

    if system == "Darwin":  # macOS
        return str(home / "Library" / "Application Support" / "Google" / "Chrome")
    elif system == "Windows":
        return str(home / "AppData" / "Local" / "Google" / "Chrome" / "User Data")
    elif system == "Linux":
        return str(home / ".config" / "google-chrome")
    else:
        raise RuntimeError(f"Unsupported platform: {system}")


class PlaywrightRequester(Requester):

    def __init__(
            self,
            user_data_dir: Optional[str] = None,
            profile_dir: str = "Default",
            headless: bool = True
    ):
        self.user_data_dir = user_data_dir or get_default_chrome_user_data_dir()
        self.profile_dir = profile_dir
        self.headless = headless
        self._context: Optional[BrowserContext] = None
        self._playwright = None

    def __enter__(self):
        self._playwright = sync_playwright().start()
        self._context = self._playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            channel="chrome",
            headless=self.headless,
            args=[f"--profile-directory={self.profile_dir}"],
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._context:
            self._context.close()
        if self._playwright:
            self._playwright.stop()

    def get(
            self,
            url: str,
            headers: Optional[Dict[str, Any]] = None,
            params: Optional[Dict[str, Any]] = None,
            cookies: Optional[Dict[str, Any]] = None
    ) -> "PlaywrightResponse":
        if not self._context:
            raise RuntimeError("PlaywrightRequester must be used as context manager")

        page = self._context.new_page()

        if headers:
            page.set_extra_http_headers(headers)

        # Build URL with params
        if params:
            from urllib.parse import urlencode
            url = f"{url}?{urlencode(params)}"

        response = page.goto(url, wait_until="domcontentloaded")
        content = page.content()
        breakpoint()
        page.close()

        return PlaywrightResponse(
            status_code=response.status if response else 0,
            content=content,
            url=url
        )

    def get_cookies(self, url: str) -> List[Dict[str, Any]]:
        if not self._context:
            raise RuntimeError("PlaywrightRequester must be used as context manager")
        return self._context.cookies(url)

    def get_user_agent(self) -> str:
        if not self._context:
            raise RuntimeError("PlaywrightRequester must be used as context manager")
        page = self._context.new_page()
        ua = page.evaluate("() => navigator.userAgent")
        page.close()
        return ua


class PlaywrightResponse:

    def __init__(self, status_code: int, content: str, url: str):
        self.status_code = status_code
        self.content = content
        self.url = url
        self.text = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP Error {self.status_code} for {self.url}")

    def json(self):
        import json
        return json.loads(self.content)
