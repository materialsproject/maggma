"""
Example builders for testing and general use.
"""
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime

from maggma.builders import MapBuilder,CopyBuilder
from monty.dev import deprecated

@deprecated(message="MapBuilder and CopyBuilder have been moved to the main builders module")