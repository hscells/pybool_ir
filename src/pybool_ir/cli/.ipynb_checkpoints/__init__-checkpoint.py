"""
Command line interface for pybool_ir.
"""

from pathlib import Path
from typing import List

import click
from tqdm.auto import tqdm

import pybool_ir
from pybool_ir.index.generic import GenericSearcher
from pybool_ir.query import GenericQueryParser

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=pybool_ir.__version__)
def cli():
    """
    pybool_ir utilities.
    """


@cli.group()
def pubmed():
    """Pubmed related commands."""


@cli.group()
def csur():
    """CSUR related commands."""


@cli.group()
def generic():
    """Commands for indexing and searching arbitrary collections."""


@cli.group()
def ir_datasets():
    """Commands for working with collections in ir_datasets."""


@cli.group()
def experiment():
    """Commands for doing experiments."""


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
    from pybool_ir.datasets.pubmed.baseline import download_baseline
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
    from pybool_ir.index.pubmed import PubmedIndexer
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
    from pybool_ir.index.pubmed import PubmedIndexer
    with PubmedIndexer(Path(index_path), store_fields=store_fields) as ix:
        ix.bulk_index(Path(baseline_path))


@ir_datasets.command("index")
@click.option(
    "-c",
    "--collection-name",
    "collection_name",
    type=click.STRING,
    multiple=False,
    required=True,
    help="name of the ir_datasets collection"
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
    default=True,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to store fields or not"
)
@click.option(
    "--term-vectors",
    "term_vectors",
    default=False,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to store term vectors or not"
)
def ir_datasets_index(collection_name: str, index_path: Path, store_fields: bool, term_vectors: bool):
    from pybool_ir.index.ir_datasets import IRDatasetsIndexer
    with IRDatasetsIndexer(index_path, collection_name, store_fields=store_fields, store_termvectors=term_vectors) as ix:
        ix.bulk_index()


@experiment.command("retrieval")
@click.option(
    "-c",
    "--collection-name",
    "collection_name",
    type=click.STRING,
    multiple=False,
    required=True,
    help="name of the ir_datasets collection"
)
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
    "-r",
    "--run-path",
    "run_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location to write the run file"
)
@click.option(
    "-e",
    "--evaluation-measures",
    "evaluation_measures",
    type=click.STRING,
    multiple=True,
    required=False,
    help="evaluate the run using the specified measures"
)
def ir_datasets_experiment(collection_name: str, index_path: Path, run_path: Path, evaluation_measures: List[str]):
    from pybool_ir.experiments.collections import load_collection
    from pybool_ir.experiments.retrieval import RetrievalExperiment
    import ir_measures
    possible_measures = ir_measures.measures.registry
    chosen_measures = []
    for measure in evaluation_measures:
        if measure not in possible_measures:
            raise ValueError(f"Invalid evaluation measure {measure}. Possible measures are {possible_measures}")
        chosen_measures.append(possible_measures[measure])

    collection = load_collection(collection_name)
    with RetrievalExperiment(GenericSearcher(index_path), collection,
                             query_parser=GenericQueryParser(),
                             run_path=run_path,
                             ignore_dates=True) as exp:
        run = exp.run
    if len(chosen_measures) > 0:
        print(ir_measures.calc_aggregate(chosen_measures, collection.qrels, run))


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
    from pybool_ir.index.pubmed import PubmedIndexer
    from pybool_ir.query.pubmed.parser import PubmedQueryParser
    from prompt_toolkit import PromptSession
    from prompt_toolkit.validation import Validator
    from prompt_toolkit.validation import ValidationError

    parser = PubmedQueryParser()

    class QueryValidator(Validator):
        def validate(self, query):
            text = query.text
            try:
                parser._parse(text)
            except Exception as e:
                raise ValidationError(message=str(e), cursor_position=-1)

    with PubmedIndexer(Path(index_path), store_fields=store_fields) as ix:
        print(f"pybool_ir {pybool_ir.__version__}")
        print(f"loaded: {ix.index_path}")
        session = PromptSession()
        while True:
            raw_query = session.prompt("?>", validator=QueryValidator())
            lucene_query = parser.parse_lucene(raw_query)
            ix.search_fmt(lucene_query)


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
    from pybool_ir.datasets.csur.parser import read_folder
    with open(Path(output_path), "w") as f:
        for review in read_folder(Path(raw_path)):
            f.write(f"{review.to_json()}\n")


@generic.command("search")
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
def generic_search(index_path: Path, store_fields: bool):
    from pybool_ir.index.generic import GenericSearcher
    from pybool_ir.query import GenericQueryParser
    from prompt_toolkit import PromptSession
    from prompt_toolkit.validation import Validator
    from prompt_toolkit.validation import ValidationError

    parser = GenericQueryParser()

    class QueryValidator(Validator):
        def validate(self, query):
            text = query.text
            try:
                parser.parse(text)
            except Exception as e:
                raise ValidationError(message=str(e), cursor_position=-1)

    with GenericSearcher(Path(index_path), store_fields=store_fields) as ix:
        print(f"pybool_ir {pybool_ir.__version__}")
        print(f"loaded: {ix.index_path}")
        session = PromptSession()
        while True:
            raw_query = session.prompt("?>", validator=QueryValidator())
            lucene_query = parser.parse_lucene(raw_query)
            ix.search_fmt(lucene_query)

@generic.command("jsonl_index")
@click.option(
    "-p",
    "--path",
    "raw_path",
    type=click.Path(),
    multiple=False,
    required=True,
    help="location of raw data"
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
    "-f",
    "--field",
    "fields",
    default=None,
    type=click.STRING,
    multiple=True,
    required=True,
    help="fields to store"
)
@click.option(
    "-s",
    "--store",
    "store_fields",
    default=True,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to store fields or not"
)
@click.option(
    "--term-vectors",
    "term_vectors",
    default=False,
    type=click.BOOL,
    multiple=False,
    required=False,
    help="whether to store term vectors or not"
)
def generic_index_jsonl(raw_path: Path, index_path: Path, store_fields: bool, term_vectors: bool, fields: List[str]):
    from pybool_ir.index.generic import JsonlIndexer
    with JsonlIndexer(index_path, store_termvectors=term_vectors, store_fields=store_fields, optional_fields=fields) as ix:
        ix.bulk_index(raw_path)
