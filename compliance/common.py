import argparse

from compliance.elements.project import Project


def create_report(dataset, args):
    if isinstance(args, argparse.Namespace):
        args = vars(args)
    if isinstance(args, dict):
        myproject = Project(dataset, **args)
        myproject.check_compliance()
        myproject.generate_report()
    else:
        raise TypeError("Unsupported arguments. Expects either a Namespace or dict")

