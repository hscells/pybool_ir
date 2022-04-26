from pathlib import Path

import click
from tqdm.auto import tqdm

import hyperbool

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=hyperbool.__version__)
def cli():
    """
    hyperbool indexing command.
    """


@cli.command("download-baseline")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    help="location to download Pubmed baseline"
)
def download_baseline(baseline_path: Path):
    from hyperbool.pubmed.baseline import download_baseline
    download_baseline(Path(baseline_path))


@cli.command("process")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    help="location of baseline download"
)
@click.option(
    "-o",
    "--output",
    "output_path",
    type=click.Path(),
    multiple=False,
    help="location to write processed file"
)
def process_baseline(baseline_path: Path, output_path: Path):
    from hyperbool.pubmed.index import read_folder
    with open(Path(output_path), "w") as f:
        for article in tqdm(read_folder(Path(baseline_path)), desc="articles processed", position=1):
            f.write(f"{article.to_json()}\n")


@cli.command("index")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    help="location of baseline download"
)
@click.option(
    "-i",
    "--index",
    "index_path",
    type=click.Path(),
    multiple=False,
    help="location to write the lucene index"
)
def index_baseline(baseline_path: Path, index_path: Path):
    from hyperbool.pubmed.index import Index
    with Index(Path(index_path)) as ix:
        ix.bulk_index(Path(baseline_path))
