CLI usage
---------

A protocol compliance report can be generated directly from the command line
interface.  The following is an example of generating a protocol compliance report

For a DICOM dataset::

    mrqa --data-source /path/to/dataset --format dicom --name my_dataset

For a BIDS dataset::

    mrqa --data-source /path/to/dataset --format bids --name my_dataset



API Tutorial
------------

The following is a tutorial for using the API to generate a protocol compliance.
The tutorial assumes that the user has a access to dummy dataset included with
the    `mrQA` package.
The dataset can be a single subject or a multi-subject dataset. The
tutorial will use the `mrQA` package to generate a protocol compliance report
for the dataset.

https://nbviewer.org/github/Open-Minds-Lab/mrQA/blob/master/examples/usage.ipynb
