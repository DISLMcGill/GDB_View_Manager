# Introduction
Views are widely used in relational databases to facilitate query writing, give individualized abstractions to different user groups, and improve query execution time with materialization techniques. This project explores how views could be defined and used in Neo4j by extending its query language Cypher. 

This repository contains the code of a middleware system which accepts Cypher queries and our language extensions as input, rewrites queries as necessary, and communicates with the local Neo4j database. For more details on the architecture of the middleware, you can access the paper here. 

# Project Structure

## Source Code
Our source code is under the [src folder](./src/). Follow the [instructions](./src/README.md) to setup and run the code. There is also additional detailed design documentation under this directory.

## Benchmark and experiments
You can find the queries in our benchmark under [benchmark_and_experiments](./benchmark_and_experiments/). For each of the evaluation experiments we performed (that their results are shown in [evaluation_plots](./test/evaluation_plots/)), you can also find the experiment scripts to run in order to replicate the results. Instructions on how to run the experiments can be found [here](./benchmark_and_experiments/Instructions.md). (Note that the queries included in this repository are only a subsection of our bigger benchmark that only returns nodes as return values. However, in the more recent version of this project we are also exploring queries returning paths in our benchmark.)

## Evaluation Plots
The [evaluation_plots](./test/evaluation_plots/) directory contains plots showing the performance of view creation and usage for our the subsection of our benchmark queries discussed in our paper.