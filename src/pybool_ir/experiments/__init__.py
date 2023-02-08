"""
Provides all the functionality for performing experiments using pybool_ir.
It includes classes for execuring queries, evaluating runs, and comparing runs.
It also includes ways to automatically download and process datasets.

An example of what is possible with this module:

>>> from pybool_ir.experiments.retrieval import RetrievalExperiment
>>> from pybool_ir.experiments.collections import load_collection
>>> from pybool_ir.index.pubmed import PubmedIndexer
>>> from ir_measures import *
>>> import ir_measures
>>>
>>> # Automatically downloads, then loads this collection.
>>> collection = load_collection("ielab/sysrev-seed-collection")
>>> # Point the experiment to your index, your collection.
>>> with RetrievalExperiment(PubmedIndexer(index_path="pubmed"),
...                                        collection=collection) as experiment:
...     # Get the run of the experiment.
...     # This automatically executes the queries.
...     run = experiment.run
>>> # Evaluate the run using ir_measures.
>>> ir_measures.calc_aggregate([SetP, SetR, SetF], collection.qrels, run)
"""

