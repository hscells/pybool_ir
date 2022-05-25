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


@cli.command("download-pubmed")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to download Pubmed baseline"
)
def download_baseline(baseline_path: Path):
    from hyperbool.pubmed.baseline import download_baseline
    download_baseline(Path(baseline_path))


@cli.command("process-pubmed")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location of baseline download"
)
@click.option(
    "-o",
    "--output",
    "output_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to write processed file"
)
def process_baseline(baseline_path: Path, output_path: Path):
    from hyperbool.pubmed.index import read_folder
    with open(Path(output_path), "w") as f:
        for article in tqdm(read_folder(Path(baseline_path)), desc="articles processed", position=1):
            f.write(f"{article.to_json()}\n")


@cli.command("index-pubmed")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location of baseline download"
)
@click.option(
    "-i",
    "--index",
    "index_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to write the lucene index"
)
@click.option(
    "-s",
    "--store",
    "store_fields",
    default=False,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to store fields or not"
)
def index_baseline(baseline_path: Path, index_path: Path, store_fields: bool):
    from hyperbool.pubmed.index import Index
    with Index(Path(index_path), store_fields=store_fields) as ix:
        ix.bulk_index(Path(baseline_path))


@cli.command("search-pubmed")
@click.option(
    "-i",
    "--index",
    "index_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to the lucene index"
)
def search(index_path: Path):
    from hyperbool.pubmed.index import Index
    from hyperbool.query.parser import PubmedQueryParser
    from prompt_toolkit import PromptSession
    from prompt_toolkit.validation import Validator
    from prompt_toolkit.validation import ValidationError

    parser = PubmedQueryParser()

    class QueryValidator(Validator):
        def validate(self, query):
            text = query.text
            try:
                parser.parse(text)
            except Exception as e:
                raise ValidationError(message=str(e), cursor_position=-1)

    with Index(Path(index_path)) as ix:
        print(f"hyperbool {hyperbool.__version__}")
        print(f"loaded: {ix.index_path}")
        session = PromptSession()
        while True:
            raw_query = session.prompt("?>", validator=QueryValidator())
            lucene_query = parser.parse_lucene(raw_query)
            ix.search(lucene_query)
