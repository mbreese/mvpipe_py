MVPipe - minimum viable pipeline
====

Make for HPC clusters pipelines
----

workingtitle is a replacement for the venerable "make" that aims to automate
the process of running complex analysis pipelines on high-throughput clusters.
Make is a standard build tool for compiling software. The power of make is that 
it takes a set of instructions for building various files, and given a target
output, make will determine what commands need to be run in order to build those outputs.
It basically answers the questions: what inputs are needed? and how do I build those inputs?
workingtitle aims to perform a similar function for pipelines that run on high-
throughput/performance clusters. These pipelines can be quite 
elaborate, but by focusing on specific transformations, you can make the 
pipeline easier to understand.

Make is a wonderful tool, but it is sometimes difficult to integrate into 
bioinformatics pipelines. Bioinformatics pipelines are typically run on an
HPC/HTC cluster using a batch scheduler. Each job could have different IO, 
memory, or CPU requirements. Finally, each pipeline could have to run on 
different clusters. Currently, only SGE/Open Grid includes a grid-aware make
replacement.
