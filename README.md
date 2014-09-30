Pilot - make for bioinformatics pipelines
====

Pilot is a bioinformatics pipeline replacement for the venerable "make". 
Make is the standard build tool for taking a known set of outputs and 
figuring out what commands need to be run in order to  get those outputs: 
what inputs are needed? what build graph is required? Pilot aims to perform
a similar function for bioinformatics pipelines. These pipelines can be quite 
elaborate, but by focusing on specific transformations, you can make the 
pipeline easier to understand.

Make is a wonderful tool, but it is sometimes difficult to integrate into 
bioinformatics pipelines. Bioinformatics pipelines are typically run on an
HPC/HTC cluster using a batch scheduler. Each job could have different IO, 
memory, or CPU requirements. Finally, each pipeline could have to run on 
different clusters. Currently, only SGE/Open Grid includes a grid-aware make
replacement.

