import multiprocessing as mp
import argparse

from scrapers.hilton import main as hilton_main
from scrapers.ihg import main as ihg_main
from scrapers.marriott import main as marriott_main
from scrapers.hyatt import main as hyatt_main


SCRAPERS = {
    "hilton": hilton_main,
    "ihg": ihg_main,
    "marriott": marriott_main,
    "hyatt": hyatt_main,
}


def run_single(name):
    print(f"▶ Starting {name} scraper")
    SCRAPERS[name]()
    print(f"✔ Finished {name} scraper")


def run_parallel(names):
    processes = []

    for name in names:
        p = mp.Process(target=run_single, args=(name,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hotel Scraper Runner")
    parser.add_argument(
        "--scraper",
        choices=SCRAPERS.keys(),
        help="Run a single scraper"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all scrapers in parallel"
    )

    args = parser.parse_args()

    if args.scraper:
        run_single(args.scraper)

    elif args.all:
        run_parallel(SCRAPERS.keys())

    else:
        print("❌ No option provided")
        print("Examples:")
        print("  python main.py --scraper hilton")
        print("  python main.py --all")
