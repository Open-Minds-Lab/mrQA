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
	mrqa -d /home/sinhah/scan_data/vertical_abcd -n 	ABCD -o /home/sinhah/scan_data/vertical_abcd_mrqa_files/ --config	/home/sinhah/github/mrQA/examples/mri-config-abcd.json --mrds-pkl-path  /media/sinhah/extremessd/ABCD/active_series/non-recommended/fmap/dicom_mrqa_v2_files/reports/abcd-fmap-baseline-non-recommended_DATE_11_01_2023_13_49_13.mrds.pkl #/home/sinhah/scan_data/vertical_abcd_mrqa_files/abcd-vertical_DATE_10_26_2023_16_14_18.mrds.pkl
	$(BROWSER)  /home/sinhah/scan_data/vertical_abcd_mrqa_files/
