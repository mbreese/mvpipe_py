MVpipe - minimum viable pipeline
====

Make for HPC analysis pipelines
----

MVpipe is a replacement for the venerable "make" that aims to automate
the process of running complex analysis pipelines on high-throughput clusters.
Make is a standard build tool for compiling software. The power of make is
that it takes a set of instructions for building various files, and given a
target output, make will determine what commands need to be run in order to
build those outputs. It basically answers the questions: what inputs are
needed? and how do I build those inputs? MVpipe aims to perform a similar
function for pipelines that run on high-throughput/performance clusters.
These pipelines can be quite elaborate, but by focusing on specific
transformations, you can make the pipeline easier to create and run.

Make is a powerful tool, but it is sometimes difficult to integrate into 
bioinformatics pipelines. Bioinformatics pipelines are typically run on an
HPC/HTC cluster using a batch scheduler. Each job could have different IO, 
memory, or CPU requirements. Finally, each pipeline could have to run on 
different clusters. Currently, only SGE/OGE and Grid Engine derivatives
include a grid-aware make replacement. However, it is primarily aimed at
directly building Makefiles, and that is somewhat limiting for more
complicated pipelines.

# Pipeline file syntax

## Evaluated lines
Any line that starts with `#$` will be evaluated as a MVpipe expression. All
other lines will be processed for variable stubstitutions and either
written to the log file, or included in the target script.

## Variables

`#$ foo = bar` Set a variable

`#$ foo =? bar` Set a variable if it hasn't already been set

`#$ foo += bar` Append a value to a list (if the variable has already been set,
then this will convert that variable to a list)

`#$ unset foo` Unsets a variable. Note: if the variable was used by a target,
it will still be set within the context of the target.

Variables may also be set at the command-line like this: `mvpipe -foo bar -baz 1 -baz 2`.
This is the same as saying:

    #$ foo = bar
    #$ baz += 1
    #$ baz += 2

## If/Then

Basic syntax:

    #$ if [condition]
       do something...
    #$ else
       do something else...
    #$ endif

If clauses can be nested as needed, but you can only specify one clause at a
time (there is no concept of foo==1 and bar==2)

### Clauses

`#$ if ${foo}` - if the variable ${foo} was set

`#$ if ${foo} == bar` - if the variable ${foo} equals the string "bar"
`#$ if ${foo} != bar` - if the variable ${foo} doesn't equal the string "bar"

`#$ if ${foo} < 1`    
`#$ if ${foo} <= 1`    
`#$ if ${foo} > 1`    
`#$ if ${foo} >= 1`    


## For loops

Basic syntax:

    #$ for i in {start}..{end}
       do something...
    #$ done

    #$ for i in ${list}
       do something...
    #$ done

## Target definitions

Targets are defined by...

## Variable substitution

    ${var}          - Variable named "var". If "var" is a list, ${var} will
                      be replaced with a space-delimited string with all
                      members of the list

    foo_@{var}_bar  - An replacement list, capturing the surrounding context.
                      For each member of list, the following will be returned:
                      foo_one_bar, foo_two_bar, foo_three_bar, etc...

    foo_@{n..m}_bar - An replacement range, capturing the surrounding context.
                      For each member of range ({n} to {m}, the following will
                      be returned: foo_1_bar, foo_2_bar, foo_3_bar, etc...

                      {n} and {m} may be variables or integers

## Target substitutions
In addition to global variable substitutions, within a target, these
additional substitutions are available. Targets may also have their own
local variables.

Note: For global variables, their values are captured when a target is
defined.

    $>              - The list of all outputs
    $>num           - The {num}'th output (starts at 1)

    $<              - The list of all inputs
    $<num           - The {num}'th input (starts at 1)

    $?              - The input-group selected (if there are more than one)

    $num            - If a wildcard was matched for the target-name (%.txt,
                      for example), the wildcard for output {num}. (Each
                      output target filename can have at most one wildcard).

## Including other files
Other Pileline files can be imported into the currently running Pipeline by
using the `#$ include filename` directive. In this case, the directory of the
current Pileline file will be searched for 'filename'. If it isn't found, 
then the current working directory will be searched. If it still isn't found,
then an ParseError will be thrown.

## Logging
You can define a log file to use within the Pileline file. You can do this
with the `#$ log filename` directive. If an existing log file is active, then
it will be closed and the new log file used. By default all output from the
Pipeline will be written to the last log file specified.

You may also specify a log file from the command-line with the `-l logfile`
command-line argument.

## Comments
Comments are started with two `##` characters. If a line starts with only one
`#`, then it will be evaluated and outputed to the log file (if one exists) or
the script body for the target.

