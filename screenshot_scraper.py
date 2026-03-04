"""
Web scraping de dashboards Dune usando Playwright.

Abre cada dashboard, espera renderizar, tira screenshot da página inteira.
Salva em data/screenshots/{data}/.
"""

import asyncio
from pathlib import Path
from datetime import date

from playwright.async_api import async_playwright
from config import DASHBOARDS, SCREENSHOTS_DIR
from logger_setup import get_logger

log = get_logger(__name__)


async def scrape_all_dashboards() -> list[tuple[str, Path, str]]:
    """
    Acessa cada dashboard Dune, tira screenshots.

    Returns:
        [(dashboard_key, image_path, dashboard_key), ...]
    """
    results = []
    today = date.today().isoformat()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            log.info("Playwright browser launched (headless)")

            for dashboard_key, url in DASHBOARDS.items():
                try:
                    log.info("Scraping dashboard: %s (%s)", dashboard_key, url)
                    page = await browser.new_page()

                    # Acessa a página (load é suficiente, networkidle é muito restritivo)
                    # Timeout generoso: 5 minutos (300s)
                    await page.goto(url, wait_until="load", timeout=300000)
                    log.debug("Page loaded for %s", dashboard_key)

                    # Extra delay generoso pra SVGs/gráficos renderizarem
                    await page.wait_for_timeout(10000)

                    # Cria diretório se não existir
                    screenshot_dir = SCREENSHOTS_DIR / today
                    screenshot_dir.mkdir(parents=True, exist_ok=True)

                    # Tira screenshot da página inteira
                    screenshot_path = screenshot_dir / f"{dashboard_key}.png"
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                    log.info("Screenshot saved: %s", screenshot_path)

                    results.append((dashboard_key, screenshot_path, dashboard_key))
                    await page.close()

                except Exception as e:
                    log.error("Failed to scrape %s: %s", dashboard_key, e)
                    continue

            await browser.close()
            log.info("Browser closed. Scraped %d dashboards", len(results))

    except Exception as e:
        log.error("Playwright error: %s", e)
        return []

    return results


def scrape_all_dashboards_sync() -> list[tuple[str, Path, str]]:
    """Wrapper síncrono pra chamar o async."""
    return asyncio.run(scrape_all_dashboards())
