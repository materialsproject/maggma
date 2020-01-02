""" Simple API Interface for Maggma """
from monty.serialization import loadfn
from pathlib import Path

default_error_responses = loadfn(Path(__file__).parent / "default_responses.yaml")
