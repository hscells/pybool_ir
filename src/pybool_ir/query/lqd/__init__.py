from pathlib import Path

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.validation import Validator, ValidationError

import pybool_ir
from pybool_ir.query.lqd.parser import lqd_parse
from pybool_ir.query.lqd.eval import Environment

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
__version__ = "0.1.0"


@click.version_option(version=pybool_ir.__version__)
@click.command()
@click.argument(
    "index_path",
    type=click.Path(),
)
def search(index_path: str):
    """
    pybool_ir utilities.
    """

    class QueryValidator(Validator):
        def validate(self, query):
            text = query.text
            e, ok = lqd_parse(text)
            if not ok:
                raise ValidationError(message=str(e), cursor_position=-1)

    with Environment(Path(index_path)) as env:
        print(f"pybool_ir {pybool_ir.__version__}")
        print(f"lqd {__version__}")
        print(f"loaded: {env.index_path}")
        session = PromptSession()
        while True:
            raw_query = session.prompt("?>", validator=QueryValidator())
            try:
                v = env.eval(raw_query)
                print(v)
                if v is not None:
                    print(v.value)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    search()
