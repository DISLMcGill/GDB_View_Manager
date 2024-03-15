# Introduction
Views are widely used in relational databases to facilitate query writing, give individualized abstractions to different user groups, and improve query execution time with materialization techniques. This project explores how views could be defined and used in graph database systems (GDBS) with a similar purpose to what can be found in relational systems. 

We perform our analysis using Neo4j and its query language Cypher which has many of the features typically found in graph query languages, aiming to pave the way for integrating view management into a wider range of GDBS.

# Setting up and running the middleware

# Project Structure

## Source Code
Our source code is under the [src folder](./src/), 

## Evaluation Plots
Under [evaluation_plots](./test/evaluation_plots/) directory you can find the figures in our paper showing the performance of view creation and usage for our benchmark queries.


# Developer notes on the internal design - Data Structures 
ain.Main: nodeTable and edgeTable store as the key the view name that is used, along with the set of node or edge identifiers returned by the view. 

main.QueryParser: This walks down the tree and executes enter/exit based on which components are entered. Look for ANTLR documentation for details. There is meta-data during these enter/exit methods to keep the dependencyTable updated if it is a view creation. For view usage, variables are set up (symbols used for view use, set of conditions) so that main.Main can know which set of identifiers it can pull from the node or edge tables. For view updates, the dependency table is referenced and a set of outdated views is returned to main.Main, and most steps are commented with details/logic.

main.DependencyTable is a hashtable with a graph component label as the key (Person, PARENT_OF, Post, etc) and a main.TableEntry object as the value. main.TableEntry contains a list of main.EntryData which are associated with itself. For instance, a main.TableEntry :Post may have several main.EntryData, which differ due to the set of conditions. main.EntryData contains a condition list (which uniquely identifies it) and a list of views which depend on it. 