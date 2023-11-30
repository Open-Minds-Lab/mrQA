mrQA : automatic protocol compliance checks on MR datasets
=============================================================

.. image:: https://img.shields.io/pypi/v/mrQA.svg
        :target: https://pypi.python.org/pypi/mrQA

.. image:: https://app.codacy.com/project/badge/Grade/8cd263e1eaa0480d8fac50eba0094401
        :target: https://app.codacy.com/gh/sinhaharsh/mrQA/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade

.. image:: https://github.com/sinhaharsh/mrQA/actions/workflows/continuous-integration.yml/badge.svg
        :target: https://github.com/sinhaharsh/mrQA/actions/workflows/continuous-integration.yml


.. image:: https://raw.githubusercontent.com/jupyter/design/master/logos/Badges/nbviewer_badge.svg
        :target: https://nbviewer.org/github/Open-Minds-Lab/mrQA/blob/master/examples/usage.ipynb


----

Documentation: https://open-minds-lab.github.io/mrQA/

----

``mrQA`` is a tool developed for automatic evaluation of protocol compliance in MRI datasets. The tool analyzes MR acquisition data from DICOM headers and compares it against protocol to determine the level of compliance. It takes as input a dataset in DICOM/BIDS format. The tool outputs a compliance report in HTML format, with a percent compliance score for each sequence/modality in a dataset. The tool also outputs a JSON file with the compliance scores for each modality. In addition, it highlights any deviations from the protocol. The tool has been specifically created keeping in mind those who directly acquired the data such as MR Physicists and Technologists, but can be used by anyone who wants to evaluate that MR scans are acquired according to a pre-defined protocol and to minimize errors in acquisition process.

``mrQA`` uses ``MRDataset`` to efficiently parse various neuroimaging dataset formats, which is available `here <github.com/Open-Minds-Lab/MRdataset>`_.

Key features:

- evaluation of protocol compliance (within-sequence across-dataset) in an existing dataset
- continuous monitoring of incoming data (hourly or daily on XNAT server or similar)
- parallel processing of very large datasets (like ABCD or UK Biobank) on a HPC cluster
- few more to be released soon including vertical audit within-session across-sequence checks

Simple schematic of the library:

.. image:: ./docs/schematic_mrQA.png





