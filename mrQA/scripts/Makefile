.PHONY: abcd_subset
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

abcd_subset:
	mrqa -d /media/sinhah/extremessd/ABCD-375/dicom-baseline-subset -n 	ABCD-subset -o /home/sinhah/debug_reports/ABCD-subset/ --config	/home/sinhah/github/mrQA/mrQA/resources/mri-config-abcd.json --mrds-pkl /home/sinhah/debug_reports/ABCD-subset/ABCD-subset_DATE_10_19_2023_12_46_41.mrds.pkl

