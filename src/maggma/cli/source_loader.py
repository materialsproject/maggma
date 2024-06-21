import importlib.util
import sys
from glob import glob
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from pathlib import Path

from maggma.core import Builder

try:
    import nbformat
    from IPython import get_ipython
    from IPython.core.interactiveshell import InteractiveShell
    from regex import match
except ModuleNotFoundError:
    pass

_BASENAME = "maggma.cli.sources"


class ScriptFinder(MetaPathFinder):
    """
    Special Finder designed to find custom script builders.
    """

    @classmethod
    def find_spec(cls, fullname, path, target=None):
        if not (str(fullname).startswith(f"{_BASENAME}.")):
            return None

        # The last module is what we want to find the path for
        sub_path = str(fullname).split(".")[-1]
        segments = sub_path.split("_")

        file_path = next(find_matching_file(segments))

        if file_path is None:
            return None

        return spec_from_source(file_path)


class NotebookLoader(Loader):
    """Module Loader for Jupyter Notebooks or Source Files."""

    def __init__(self, name=None, path=None):
        self.shell = InteractiveShell.instance()

        self.name = name
        self.path = path

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        module.__dict__["get_ipython"] = get_ipython
        module.__path__ = self.path

        # load the notebook object
        with open(self.path, encoding="utf-8") as f:
            nb = nbformat.read(f, 4)

        # extra work to ensure that magics that would affect the user_ns
        # actually affect the notebook module's ns
        save_user_ns = self.shell.user_ns
        self.shell.user_ns = module.__dict__

        try:
            for cell in nb.cells:
                if cell.cell_type == "code":
                    # transform the input to executable Python
                    code = self.shell.input_transformer_manager.transform_cell(cell.source)
                    # run the code in themodule
                    exec(code, module.__dict__)
        finally:
            self.shell.user_ns = save_user_ns
        return module


def spec_from_source(file_path: str) -> ModuleSpec:
    """
    Returns a ModuleSpec from a filepath for importlib loading
    Specialized for loading python source files and notebooks into
    a temporary maggma cli package to run as a builder.
    """
    file_path_obj = Path(file_path).resolve().relative_to(Path(".").resolve())
    file_path_str = str(file_path_obj)

    if file_path_obj.parts[-1][-3:] == ".py":
        # Gets module name from the filename without the .py extension
        module_name = "_".join(file_path_obj.parts).replace(" ", "_").replace(".py", "")

        spec = ModuleSpec(
            name=f"{_BASENAME}.{module_name}",
            loader=SourceFileLoader(fullname=f"{_BASENAME}.{module_name}", path=file_path_str),
            origin=file_path_str,
        )
        # spec._set_fileattr = True
    elif file_path_obj.parts[-1][-6:] == ".ipynb":
        # Gets module name from the filename without the .ipnb extension
        module_name = "_".join(file_path_obj.parts).replace(" ", "_").replace(".ipynb", "")

        spec = ModuleSpec(
            name=f"{_BASENAME}.{module_name}",
            loader=NotebookLoader(name=f"{_BASENAME}.{module_name}", path=file_path_str),
            origin=file_path_str,
        )
        # spec._set_fileattr = True
    else:
        raise Exception("Can't load {file_path}. Must provide a python source file such as a .py or .ipynb file")

    return spec


def load_builder_from_source(file_path: str) -> list[Builder]:
    """
    Loads Maggma Builders from a Python source file.
    """
    file_path = str(Path(file_path).resolve())
    spec = spec_from_source(file_path)
    module_object = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module_object)  # type: ignore

    sys.modules[spec.name] = module_object

    if hasattr(module_object, "__builders__"):
        return module_object.__builders__
    if hasattr(module_object, "__builder__"):
        return module_object.__builder__
    raise Exception(f"No __builders__ or __builder__ attribute found in {file_path}")


def find_matching_file(segments, curr_path="./"):
    """
    Finds file that has the right sequence of segments
    in the path relative to the current path
    Requires all segments match the file path.
    """
    # If we've gotten to the end of the segment match check to see if a file exists
    if len(segments) == 0:
        if Path(curr_path + ".py").exists():
            yield curr_path + ".py"
        if Path(curr_path + ".ipynb").exists():
            yield curr_path + ".ipynb"
    else:
        # Recurse down the segment tree some more
        current_segment = segments[0]
        remainder = segments[1:]

        re = rf"({curr_path}[\s_]*{current_segment})"
        pos_matches = [match(re, pos_path) for pos_path in glob(curr_path + "*")]
        pos_matches = {pmatch.group(1) for pmatch in pos_matches if pmatch}
        for new_path in pos_matches:
            if Path(new_path).exists() and Path(new_path).is_dir:
                for sub_match in find_matching_file(remainder, curr_path=new_path + "/"):
                    yield sub_match
            for sub_match in find_matching_file(remainder, curr_path=new_path):
                yield sub_match
