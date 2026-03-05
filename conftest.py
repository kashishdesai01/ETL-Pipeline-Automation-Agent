# conftest.py — adds project root to sys.path so `core` and `agents` are importable
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
