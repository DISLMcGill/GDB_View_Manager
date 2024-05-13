# Introduction

Views are widely used in relational databases, simplifying query writing, providing tailored abstractions for different user groups, and enhancing query execution performance through materialization techniques. This project explores the integration of views into Neo4j, togetehr with extending its query language, Cypher.

This repository contains the code for a middleware system. It accepts Cypher queries and our language extensions as input, performs necessary query rewrites, and communicates with the local Neo4j database. For more details of the middleware's architecture, refer to our paper.

# Project Structure

## Source Code
Our source code is located in the [src folder](./src/). Follow the provided [instructions](./src/README.md) to set up and run the code. Detailed design documentation is also available within this directory.

## Benchmark and Experiments
Queries for our benchmark can be found in [benchmark_and_experiments](./benchmark_and_experiments/). Each evaluation experiment, with results shown in [evaluation_plots](./test/evaluation_plots/), includes corresponding scripts for regenrating the results. Detailed instructions for running these experiments are provided [here](./benchmark_and_experiments/Instructions.md). (Please note that the queries within this repository represent a subset of our larger benchmark, focusing only on node return values. However, in our latest project version, we're expanding to include queries returning paths in our benchmark.)

## Evaluation Plots
The [evaluation_plots](./test/evaluation_plots/) directory contains plots illustrating the performance of view creation and usage for the subset of benchmark queries discussed in our paper.
