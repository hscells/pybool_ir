# *hyperbool*

This repository contains many related tools and libraries for the development of systematic review automation research, focusing on [Information Retrieval](https://en.wikipedia.org/wiki/Information_retrieval) methods. This library is broken into various packages to meet the needs of different research goals. Currently, the packages are organised as follows:

 - `pubmed`: Used for indexing and querying [Pubmed](https://pubmed.ncbi.nlm.nih.gov/) data. This package can create clean-room design [lucene](https://lucene.apache.org/) indexes that attempt to totally replicate how Pubmed indexes documents.
 - `query`: Used for parsing and performing operations on Pubmed queries. Included in this package is a query parser that translates Pubmed queries into equivalent lucene queries. The queries aim to replicate the search process of Pubmed. One can also use this package to perform various operations on Pubmed queries using the parsed [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree).
 - `experiments`: This package wraps many implementation details of the above packages and provides an easy way to do retrieval experiments. This package also contains methods for downloading and using test collections that have been created for IR-focused systematic review research.

## Getting started

This library is still under much development, and I haven't yet begun to go through the process that makes installation easier. You are usually better off at the moment installing everything below and then running your notebooks within the environment that is created.

### pipenv

For dependency and environment management, I've decided to use pipenv. If you are installing things for the first time, you might need to comment out the `jcc` and `lucene` packages before you activate the environment with `pipenv shell`.

### pylucene

There is a bit of a barrier to entry at the moment in using this library. That is because many packages, particuarly the indexing and querying facilities, depend on the [pylucene](https://lucene.apache.org/pylucene/) library. Unfortunately there is some manual intervention that needs to be performed before you use `hyperbool`. I have tried to make this process as painless as possible, and it should work for mac and linux people. 

 1. Run the `install_pylucene.sh` script. This will attempt to download all the necessary files, move them to the correct spots, and then create the Python `.whl` file that contains the lucene python package (pylucene).
 2. Modify the `Pipfile` such that the `lucene` dependency points to the `.whl` file you just created.
 3. Activate your environment with `pipenv shell`. If this fails, then something has gone wrong. 

### Downloading and indexing Pubmed

Once you have activated the pipenv environment, a command will become available to handle downloading and indexing Pubmed data for you.

 1. `hbindexer download-baseline -b PUBMED_PATH`
 2. `hbindexer index -b PUBMED_PATH -i INDEX_PATH`

### Retrieval experiments

Typical retrieval experiment. Note that there are many more arguments that one can pass to the experiment to extend what the experiment does. 

```python
from hyperbool.experiments.collections import load_collection
from hyperbool.experiments.retrieval import RetrievalExperiment
from ir_measures import *
import ir_measures

# Automatically downloads, then loads this collection.
collection = load_collection("ielab/sysrev-seed-collection")
# Point the experiment to your index, your collection.
with RetrievalExperiment(index_path="pubmed", collection=collection) as experiment:
    # Get the run of the experiment.
    # This automatically executes the queries.
    run = experiment.run
# Evaluate the run using ir_measures.
ir_measures.calc_aggregate([SetP, SetR, SetF], collection.qrels, run)
```

It's also possible to do more ad hoc retrieval experiments.

```python
from hyperbool.experiments.retrieval import AdHocExperiment

with AdHocExperiment(index_path="pubmed", raw_query="empagliflozi[sh]") as experiment:
    print(experiment.count())
```

## Documentation

I am planning to have some more comprehensive documentation of all the packages in `hyperbool`. I still need to write the docstrings!

## Roadmap
 
 - Get indexing and searching as close as possible to Pubmed.
 - Build out the experiment package to provide stubs for experiments other than ad hoc retrieval (think: classification, seed driven document ranking, etc.).
 - Get and indexing pipeline for more collections, like PMC and Cochrane.
