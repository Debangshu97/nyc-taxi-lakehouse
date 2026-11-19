# Databricks notebook source
import os
import runpy
import sys

dbutils.widgets.text("step", "bronze")  # noqa: F821
step = dbutils.widgets.get("step")  # noqa: F821

repo_root = os.path.abspath("..")
sys.path.insert(0, repo_root)
os.environ["DATA_DIR"] = "/Volumes/workspace/nyc_taxi/lake/raw"
os.environ["DELTA_ROOT"] = "/Volumes/workspace/nyc_taxi/lake/delta"

_ = runpy.run_module(f"src.taxi.{step}", run_name="__main__")