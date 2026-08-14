# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("Deploy realizado via Databricks Asset Bundles com sucesso!")