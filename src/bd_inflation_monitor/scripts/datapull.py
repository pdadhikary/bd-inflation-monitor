import logging
import sys
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.logging import setup_logging

logger = logging.getLogger()


def download_excel_file(url, save_path):
    logger.info(f"Downloading {url}...")
    response = requests.get(url)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        logger.info(f"Successfully downloaded and saved to {save_path}")
    else:
        logger.error(f"Failed to download. Status code: {response.status_code}")


def datapull():
    try:
        response = requests.get(settings.bbs_url, verify=False)
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to BBS site.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occured: {e}")
        sys.exit(1)

    logger.info("Searching for this months report...")
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", class_="ck-table-resized")
    assert table is not None
    rows = table.find_all("tr")
    assert rows is not None

    months: list[str] = []
    files: list[str] = []
    for row in rows:
        cells = row.find_all("td")
        assert cells is not None

        if cells[-3].text != " ":
            months.append(cells[-3].text)
            file_link = cells[-1].find("a")

            if file_link is not None:
                files.append(str(file_link["href"]))
            else:
                files.append("")

    current_datetime = datetime.now()
    current_month = current_datetime.strftime("%B")

    file_dict = dict(zip(months[1:], files[1:]))

    if current_month in file_dict.keys():
        save_dir = Path(settings.stage_dir)
        save_path = save_dir / f"{current_datetime.strftime('%b%Y')}.xlsx"
        logger.info(f"Downlaoding report for {current_month}...")
        download_excel_file(file_dict[current_month], str(save_path))
        logger.info(f"Successfully downloaded report for {current_month}...")
    else:
        logger.info(f"Report for {current_month} is not available in the BBS site yet.")


def main():
    setup_logging()
    datapull()


if __name__ == "__main__":
    main()
