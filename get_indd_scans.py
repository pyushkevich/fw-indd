#!/bin/python
import click
import io
import flywheel
import pydicom
import sys
import csv

# A list of search paths in FlyWheel used to search for INDD subjects.
# For now this is hard-coded
search_paths = ["cfn/PMC-CLINICAL", "dwolklab/NACC-SC"]

# This function reads the first DICOM file found in an acquisition
# It also returns the FW file reference
def fw_parse_acq_dicom(acq):
  
  # Search for the first DICOM file
  try:
    f=next(x for x in acq.files if (lambda f: f.type=='dicom'))
    zi=acq.get_file_zip_info(f.name)
    fp=io.BytesIO(acq.read_file_zip_member(f.name, zi.members[0].path))
    return pydicom.dcmread(fp, stop_before_pixels=True),f
  except:
    return None,None


# This function reads dicom tags from the first acquisition
# in a FlyWheel session. Tags can then be accessed as simple
# attributes of the return object
def fw_parse_session_dicom(sess):
  
  # Get the first acquisition
  acq=sess.acquisitions.find_first()
  if acq is None:
    return None,None

  # Search for the first DICOM file
  return fw_parse_acq_dicom(acq)


# Get the modality of a session (uses first acquisition)
def fw_get_session_modality(sess):

    # Get the first acquisition
    acq = sess.acquisitions.find_first()
    if acq is not None:

        # Get the first DICOM file
        return acq.files[0].modality if len(acq.files) > 0 else None

    return None


# Get an iterator over all acquisitions of given type, optionally in a 
# given subject
def fw_make_acq_modality_filter(client, modality, project_path, subject=None):

    # Find the project id
    proj_id = client.lookup(project_path).id

    # Create the filter string
    if subject is not None:

        try:
            subj_id = client.lookup("%s/%s" % (project_path, subject)).id
            return 'files.modality=%s,parents.subject=%s,parents.project=%s' % (modality, subj_id, proj_id)
        except:
            return None

    else:

        return 'files.modality=%s,parents.project=%s' % (modality, proj_id)


# List of columns returned for each modality
modality_cols = {
    'COMMON': [
        'FlywheelSubjectID',
        'FlywheelSessionTimestamp',
        'FlywheelSessionURL',
        'FlywheelSessionInternalID',
        'FlywheelProjectInternalID',
        'FlywheelAcquisitionLabel',
        'FlywheelAcquisitionIntent',
        'FlywheelAcquisitionMeasurement',
        'FlywheelAcquisitionFeatures',
        'DicomModality',
        'DicomInstitutionName',
        'DicomStationName',
        'DicomBodyPartExamined',
        'DicomStudyInstanceUID',
        'DicomSeriesInstanceUID' ],

    'MR' : [
        'DicomMagneticFieldStrength',
        'DicomSequenceName',
        'DicomSliceThickness',
        'DicomRepetitionTime',
        'DicomEchoTime',
        'DicomFlipAngle',
        'DicomNumberOfAverages',
        'DicomSpacingBetweenSlices',
        'DicomPixelSpacing'
        ],

    'PT' : [ ]
}


# Return a string to include in the CVS file for a given column
def make_output_text(sess, fw_acq, fw_file, dcm, column):
    
    # Use dictionary for simple outputs
    action_dict = {
        'FlywheelSubjectID' : sess['subject_id'],
        'FlywheelSessionTimestamp' : sess['session_ts'],
        'FlywheelSessionInternalID' : fw_acq.parents.session,
        'FlywheelProjectInternalID' : fw_acq.parents.project,
        'FlywheelAcquisitionLabel' : fw_acq.label,
        'FlywheelSessionURL' : "https://upenn.flywheel.io/#/projects/%s/sessions/%s?tab=data" % 
            (fw_acq.parents.project,fw_acq.parents.session)
    } 

    if column in action_dict:
        val=action_dict[column]
    elif column.startswith('FlywheelAcquisition'):
        key=column[len('FlywheelAcquisition'):]
        fcl = fw_file.classification;
        val = ';'.join(fcl[key]) if key in fcl else None
    elif column.startswith('Dicom'):
        val=dcm.get(column[5:], None)
    else:
        val=None

    return "%s" % val



# Get a listing of all the MRI scans across projects
def fw_list_acq(csv_stream, client, modality, project_path, col_list, subject=None):

    # Get a filter to search for all acquisitions of this type
    fw_filter = fw_make_acq_modality_filter(client, modality, project_path, subject)

    # If no filter, then the project or subject were not found
    if fw_filter is None:
        return

    # This is a local cache for session properties
    sess_cache={}

    # Use the filter to list acquisitions
    for acq in client.acquisitions.iter_find(fw_filter):

        # Get the session for this acquisition
        sess_id = acq.parents.session
        if sess_id not in sess_cache:

            # Load the session
            sess = client.get_session(sess_id)

            # Store the common information about this session
            sess_cache[sess_id] = {
                'subject_id':sess.subject.label,
                'session_ts':sess.timestamp
            }

        # Load the dicom for this acquisition, or skip this acq if not found
        dcm,f = fw_parse_acq_dicom(acq)
        if dcm is None:
            continue

        # Get the output corresponding to each column
        f_out = lambda col:make_output_text(sess_cache[sess_id], acq, f, dcm, col)
        data = map(f_out, col_list)

        # Print a comma-separated list
        csv_stream.writerow(data);




# Main entrypoint
@click.command()
@click.option('--key', '-k', help='Path to the FlyWheel API key file', required=True)
@click.option('--subject', '-s', help='Only list information for one subject')
@click.option('--header/--no-header', '-H', help='Include a header in the CSV file', default=False)
@click.option('--modality', '-m', type=click.Choice(['MRI', 'PET'], case_sensitive=False),
              help='Which modality to list', default='MRI')
def get_indd_scans(key, subject, header, modality):
    """Get a listing of INDD scans in FlyWheel in CSV format"""

    with open(key, 'r') as keyfile:
        fw_api_key = keyfile.read().strip()
    
    # Open connection to FW
    fw = flywheel.Client(fw_api_key)

    # Map the modality to flywheel lingo
    mod_map = { 'mri' : 'MR', 'pet' : 'PT' }
    mod_fw = mod_map[modality.lower()]

    # Get the list of columns
    columns = modality_cols['COMMON'] + modality_cols[mod_fw]

    # Create the CSV
    csv_stream = csv.writer(sys.stdout)

    # Print the header if requested
    if header:
        csv_stream.writerow(columns)

    for sp in search_paths:
        fw_list_acq(csv_stream, fw, mod_fw, sp, columns, subject)











if __name__ == '__main__':
    get_indd_scans()
