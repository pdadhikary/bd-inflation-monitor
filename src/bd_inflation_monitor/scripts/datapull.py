import logging
from datetime import date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.logging import setup_logging

logger = logging.getLogger(__name__)


def download_excel_file(url: str, save_path: Path) -> None:
    logger.info(f"Downloading {url}...")
    response = requests.get(url)
    response.raise_for_status()
    save_path.write_bytes(response.content)
    logger.info(f"Saved to {save_path}")


def datapull() -> None:
    try:
        response = requests.get(settings.bbs_url, verify=False)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Could not connect to BBS site: {e}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"HTTP error fetching BBS site: {e}") from e

    logger.info("Searching for this month's report...")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="ck-table-resized")

    if table is None:
        raise ValueError(
            "Could not find expected table (class='ck-table-resized') on BBS page. "
            "The site layout may have changed."
        )

    rows = table.find_all("tr")
    months: list[str] = []
    files: list[str] = []

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue
        if cells[-3].text.strip():
            months.append(cells[-3].text.strip())
            file_link = cells[-1].find("a")
            files.append(str(file_link["href"]) if file_link else "")

    if len(months) < 2:
        raise ValueError(f"Parsed fewer rows than expected from BBS table: {months}")

    current_reporting_date = date.today().replace(day=1) - timedelta(days=1)
    current_reporting_month = current_reporting_date.strftime("%B")
    file_dict = dict(zip(months[1:], files[1:]))

    if current_reporting_month not in file_dict:
        logger.info(
            f"Report for {current_reporting_month} is not yet available on the BBS site."
        )
        return

    url = file_dict[current_reporting_month]
    if not url:
        raise ValueError(
            f"Found month entry for {current_reporting_month} but the download link is empty."
        )

    save_dir = Path(settings.stage_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{current_reporting_date.strftime('%b%Y')}.xlsx"

    logger.info(f"Downloading report for {current_reporting_month}...")
    download_excel_file(url, save_path)
    logger.info(f"Successfully downloaded report for {current_reporting_month}.")


def main():
    setup_logging()
    datapull()


if __name__ == "__main__":
    main()
