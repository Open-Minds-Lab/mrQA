Examples using API
==================

The API can be used in a variety of ways to analyze the dataset for generating
compliance reports. The page describes three typical use-cases for the API

General Use-Case
----------------
The most common use case for the API is to generate compliance reports for a single
MRI dataset. In this scenario, a user would typically access the API through
command-line interface or by writing python scripts. The user should provide the
API with the location of the dataset, as well as any relevant metadata or
additional arguments. The API would then analyze the dataset and generate a report
indicating which modalities/subjects/sessions conform to the specified protocol

On the CLI, specify the arguments as given below::

    mrqa --data-source /path/to/dataset --style dicom --name my_dicom_dataset

To check for a BIDS dataset::

    mrqa --data-source /path/to/dataset --style bids --name my_bids_dataset

Similarly, in a python script::

    from MRdataset import import_dataset
    from mrQA import check_compliance

    data_folder = "/home/datasets/XYZ_dataset"
    output_dir = '/home/mr_reports/XYZ'

    dicom_dataset = import_dataset(data_source=data_folder,
                                   style='dicom',
                                   name='XYZ_study')
    report_path = check_compliance(dataset=dicom_dataset,
                                   output_dir=args.output_dir)

To check for a BIDS dataset, use `style` argument::

    bids_dataset = import_dataset(data_source=data_folder,
                                   style='bids',
                                   name='XYZ_study')
    report_path = check_compliance(dataset=bids_dataset,
                                   output_dir=args.output_dir)

Parallel Use-Case
-----------------
In some cases a user may need to generate compliance reports for a large MR-dataset.
Processing large dicom datasets may be limited by disk reading speed, all the more
when a user is accessing the data over a network. Typically, the
API takes an hour to read 100 thousand .dcm files. In this scenario, we recommend
that the user should split his dataset, and read it in parallel. Then the subsets
can be merged to a single dataset and checked for compliance.

