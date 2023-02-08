# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys, os

sys.path.insert(0, os.path.abspath('../../src'))

# Required... for some reason?
import pybool_ir
import pybool_ir.experiments.collections
import pybool_ir.experiments.decompose
import pybool_ir.experiments.retrieval
from pybool_ir.query.pubmed.parser import PubmedQueryParser

try:
    import builtins
except ImportError:
    import builtins as builtins  # type: ignore

PATH_ROOT = os.path.dirname(__file__)
builtins.__PYBOOLIR_SKIP__ = True  # type: ignore

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'pybool_ir'
copyright = '2023, Harry Scells'
author = 'Harry Scells'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
]
autosummary_generate = True
autodoc_mock_imports = []
templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']


autodoc_default_options = {
    "members": True,
    "methods": True,
    "special-members": "__call__",
    "exclude-members": "_abc_impl",
    "show-inheritance": True,
}
