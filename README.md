# pybool_ir

**UPDATE!** [pybool_ir-ui](https://github.com/hscells/pybool_ir-ui) is a new tool for creating IR demos with indexes and query languages created in pybool_ir. You can see a live demo of a PubMed index created in pybool_ir which uses the PubMed query syntax at [pubmed.chatnoir.eu](https://pubmed.chatnoir.eu).

This repository contains many related tools and libraries for the development of domain-specific  [information retrieval](https://en.wikipedia.org/wiki/Information_retrieval) research, with a focus on accurate indexing of complex collections and experimenting with query languages (namely variations on Boolean query syntax). This library is broken into various packages to meet the needs of different research goals. Some notable modules include:

 - `query`: Used for parsing and performing operations on queries. Included in this package is a query parser that translates queries for search engines like PubMed into equivalent Lucene queries. One can also use this package to perform various operations on queries using the parsed [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree).
 - `experiments`: This package wraps provides an easy way to do retrieval experiments. This package also contains methods for downloading and using test collections that have been created for information retrieval-focused domain-specific research. pybool_ir also supports the catalog of collections made available by [ir_datasets](https://ir-datasets.com/).

## Getting started

**UPDATE!** I've started another project that pre-builds pylucene for you, making it pip-installable. Unfortunately due to limitations in the Python packaging ecosystem, it's still not as easy as `pip install pylucene`... For installation instructions, see: [pylucene-packaged](https://github.com/hscells/pylucene-packaged).

The following instructions will get you a copy of the project up and running on your local machine for development and research purposes. The project can currently be installed locally, or using a docker image. We are still working on getting the library easily installable via PyPI.

Once you have everything installed, you can browse the [documentation](https://scells.me/pybool_ir/) for more information on using the library.

### pipenv

For dependency and environment management, I've decided to use pipenv. Please read the following sections before attempting to use it.

### pylucene

There is a bit of a barrier to entry at the moment in using this library. That is because many sub-packages, particularly the indexing and querying facilities, depend on the [pylucene](https://lucene.apache.org/pylucene/) library. Unfortunately there is some manual intervention that needs to be performed before you use `pybool_ir`. We have tried to make this process as painless as possible, and it should work for mac and linux people.

 1. Run `pipenv shell` to create a new environment in this directory.
 2. Run the `install_pylucene.sh` script. This will attempt to download all the necessary files, move them to the correct spots, and then create the Python `.whl` file that contains the lucene python package (pylucene). It also installs this package into the pipenv environment (for local development). If you are using another environment manager you should install the `.whl` file that contains pylucene and then install the pybool_ir package (e.g., using the `setup.py`). If the installation with pipenv fails, then you need to edit the path to pylucene in the Pipfile.
 3. Activate your environment with `pipenv shell`. If this fails, then something has gone wrong. 

Alternatively, we provide a Dockerfile for building a reproducible Docker image. Simply run `docker build -t pybool_ir ./docker` to build the image and run `docker run --rm pybool_ir bash -i` to start a container.

## Basic Usage

The next sections show some basic usage of the library. For more information, please see the [documentation](https://scells.me/pybool_ir/).

### Downloading and indexing Pubmed

Once you have activated the pipenv environment, the `pybool_ir` command will become available to you. 

One example of the functionality provided by this tool is to handle downloading and indexing Pubmed data for you.

 1. `pybool_ir pubmed download -b PUBMED_PATH`
 2. `pybool_ir pubmed index -b PUBMED_PATH -i INDEX_PATH`
 3. Once indexed, you can test to see if everything is working by running `pybool_ir pubmed search -i INDEX_PATH`

**Note** Please see the full options for each of these commands using the `-h` parameter to find out how to perform additional actions, like indexing with stored fields.

### Retrieval experiments

Typical retrieval experiment. Note that there are many more arguments that one can pass to the experiment to extend what the experiment does.

```python
from pybool_ir.experiments.collections import load_collection
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.index.pubmed import PubmedIndexer
from ir_measures import *
import ir_measures

# Automatically downloads, then loads this collection.
collection = load_collection("ielab/sysrev-seed-collection")
# Point the experiment to your index, your collection.
with RetrievalExperiment(PubmedIndexer(index_path="pubmed"), collection=collection) as experiment:
    # Get the run of the experiment.
    # This automatically executes the queries.
    run = experiment.run
# Evaluate the run using ir_measures.
ir_measures.calc_aggregate([SetP, SetR, SetF], collection.qrels, run)
```

It's also possible to do more ad hoc retrieval experiments.

```python
from pybool_ir.experiments.retrieval import AdHocExperiment
from pybool_ir.index.pubmed import PubmedIndexer

with AdHocExperiment(PubmedIndexer(index_path="pubmed"), raw_query="headache[tiab]") as experiment:
    print(experiment.count())
```

## Citing

If you use this library in your research, please cite [the following paper](https://dl.acm.org/doi/10.1145/3539618.3591819):

```
@inproceedings{scells2023pyboolir,
    author = {Scells, Harrisen and Potthast, Martin},
    title = {Pybool_ir: A Toolkit for Domain-Specific Search Experiments},
    year = {2023},
    booktitle = {Proceedings of the 46th International ACM SIGIR Conference on Research and Development in Information Retrieval},
    pages = {3190–3194},
}
```