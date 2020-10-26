#!/bin/bash

# Working dir
cd /e/tk/flywheel/fw-indd

# Timestamp
TS=$(date +%Y-%m-%d)

# Create CSV file
python get_indd_scans.py -m PET \
	-k ~/.flywheel/fw_api_key -H \
	-l logs/log_pet_${TS}.txt -c cache \
	> csv/indd_pet_${TS}.csv

# Copy CSV file
if [[ $? -eq 0 ]]; then
	if [[ -f csv/indd_pet_${TS}.csv ]]; then
		dos2unix -n csv/indd_pet_${TS}.csv /s/indd_pet_${TS}.csv
	fi
fi

# Repeat for PET
python get_indd_scans.py -m MRI \
	-k ~/.flywheel/fw_api_key -H \
	-l logs/log_mri_${TS}.txt -c cache \
	> csv/indd_mri_${TS}.csv

if [[ $? -eq 0 ]]; then
	if [[ -f csv/indd_mri_${TS}.csv ]]; then
		dos2unix -n csv/indd_mri_${TS}.csv /s/indd_mri_${TS}.csv
	fi
fi
