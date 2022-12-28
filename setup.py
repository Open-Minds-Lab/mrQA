#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

import versioneer

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "mrdataset>=0.1.3",
    "pydicom",
    "nibabel",
    "dictdiffer",
    "jinja2>=3.0.3",
]

test_requirements = ['pytest>=3', 'hypothesis']

setup(
    author="Pradeep Raamana",
    author_email='raamana@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="mrQA suite of tools offering automatic evaluation of "
                "protocol compliance",
    entry_points={
        'console_scripts': [
            'protocol_compliance=mrQA.cli:main',
            'mr_proto_compl=mrQA.cli:main',
            'mrpc_subset=mrQA.run_subset:main'
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='mrQA',
    name='mrQA',
    packages=find_packages(include=['mrQA', 'mrQA.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/Open-Minds-Lab/mrQA',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    zip_safe=False,
    package_data={'mrQA': ['layout.html']}
)
