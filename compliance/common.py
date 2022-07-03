import argparse
import logging
from compliance.elements.project import Project
from pathlib import Path


def create_report(dataset=None,
                  strategy='first',
                  output_dir=None,
                  reference_path=None,
                  reindex=False,
                  verbose=False):
    if output_dir is None:
        output_dir = dataset.data_root

    myproject = Project(dataset=dataset,
                        strategy=strategy,
                        output_dir=output_dir,
                        reference_path=reference_path,
                        reindex=reindex,
                        verbose=verbose)
    myproject.check_compliance()
    myproject.generate_report()


def set_logging(metadata_root, level):
    logging.basicConfig(filename=Path(metadata_root) / 'execution.log',
                        format='%(asctime)s | %(levelname)s: %(message)s',
                        level=level)
