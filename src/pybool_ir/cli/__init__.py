from pathlib import Path

import click
from tqdm.auto import tqdm

import pybool_ir

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=pybool_ir.__version__)
def cli():
    """
    hyperbool utilities.
    """


@cli.group()
def pubmed():
    """Pubmed related commands."""


@cli.group()
def csur():
    """CSUR related commands."""


@pubmed.command("download")
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to download Pubmed baseline"
)
def pubmed_download(baseline_path: Path):
    from pybool_ir.pubmed.baseline import download_baseline
    download_baseline(Path(baseline_path))


@pubmed.command("process")
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
def pubmed_process(baseline_path: Path, output_path: Path):
    from pybool_ir.pubmed.index import PubmedIndexer
    with open(Path(output_path), "w") as f:
        for article in tqdm(PubmedIndexer.read_folder(Path(baseline_path)), desc="articles processed", position=1):
            f.write(f"{article.to_json()}\n")


@pubmed.command("index")
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
def pubmed_index(baseline_path: Path, index_path: Path, store_fields: bool):
    from pybool_ir.pubmed.index import PubmedIndexer
    with PubmedIndexer(Path(index_path), store_fields=store_fields) as ix:
        ix.bulk_index(Path(baseline_path))


@pubmed.command("search")
@click.option(
    "-i",
    "--index",
    "index_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to the lucene index"
)
@click.option(
    "-s",
    "--store",
    "store_fields",
    default=False,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to display stored fields or not"
)
def pubmed_search(index_path: Path, store_fields: bool):
    from pybool_ir.pubmed.index import PubmedIndexer
    from pybool_ir.query.parser import PubmedQueryParser
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

    with PubmedIndexer(Path(index_path), store_fields=store_fields) as ix:
        print(f"hyperbool {pybool_ir.__version__}")
        print(f"loaded: {ix.index_path}")
        session = PromptSession()
        while True:
            raw_query = session.prompt("?>", validator=QueryValidator())
            lucene_query = parser.parse_lucene(raw_query)
            ix.search(lucene_query)


@csur.command("process")
@click.option(
    "-r",
    "--raw",
    "raw_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location of raw csur download"
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
def csur_process(raw_path: Path, output_path: Path):
    from pybool_ir.csur.parser import read_folder
    with open(Path(output_path), "w") as f:
        for review in read_folder(Path(raw_path)):
            f.write(f"{review.to_json()}\n")



