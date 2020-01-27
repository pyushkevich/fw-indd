#!/bin/python
import click
import io
import flywheel
import pydicom
import sys
import csv
import re
import logging
import os
import json
import datetime

# To support JSON dump
def json_default_conv(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

# A list of search paths in FlyWheel used to search for INDD subjects.
# For now this is hard-coded
search_paths = ["cfn/PMC-CLINICAL", "dwolklab/NACC-SC"]

# This function reads the first DICOM file found in an acquisition
# It also returns the FW file reference
def fw_parse_acq_dicom(acq, dicom_cache=None):

    # Check if the DCM exists in the cache
    if dicom_cache:
        fn_cache = os.path.join(dicom_cache, '%s.json' % acq.id)
        if os.path.exists(fn_cache):
            with open(fn_cache) as f_cache:
                try:
                    cached = json.load(f_cache)
                    if cached.get('modtime', 0) == acq.modified.toordinal():
                        # Load cached dicom and file
                        dcm = pydicom.dataset.Dataset.from_json(cached.get('dicom', None))
                        f = cached.get('file', None)
                        return dcm,f
                except (ValueError):
                    pass

    # Otherwise, read the DICOM from zip file
    try:
        # Get the first DICOM file
        f=next(x for x in acq.files if (lambda f: f.type=='dicom'))
        zi=acq.get_file_zip_info(f.name)
        fp=io.BytesIO(acq.read_file_zip_member(f.name, zi.members[0].path))

        # Read the DICOM
        dcm = pydicom.dcmread(fp, stop_before_pixels=True)

        # Place into cache if defined
        if dicom_cache:
            fn_cache = os.path.join(dicom_cache, '%s.json' % acq.id)
            with open(fn_cache, 'wt') as f_cache:
                cached = {
                    'dicom': dcm.to_json(),
                    'file': f.to_dict(),
                    'modtime': acq.modified.toordinal()
                };
                json.dump(cached, f_cache, default=json_default_conv)

        # Return result
        return dcm, f

    except (KeyboardInterrupt, SystemExit):
        raise

    except (StopIteration):
        logging.warning('No DICOM files in acquisition %s' % acq.id)
        return None,None
 
    except:
        logging.warning('FlyWheel API Exception in acquisition %s' % acq.id)
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


# Function to filter INDD ids
def fn_filter_inddid(subject_name):

  # Strip INDD and IND from name
  z = re.search('^IND{1,2}(.*)$',subject_name)
  if z is not None:
    subject_name = z.group(1)

  # Try basic 6-digit pattern
  z = re.search('^[0-9]{6}$',subject_name)
  if z is not None:
    return z.group()

  # Try 8-digit pattern
  z = re.search('^([0-9]{6})[\._\-]([0-9]{2})$',subject_name)
  if z is not None:
    return '%s.%s' % (z.group(1), z.group(2))

  # Failed to match anything
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
    # Referring Physician, etc
    'COMMON': [
        'INDDID',
        'FlywheelSubjectID',
        'FlywheelSessionTimestampUTC',
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
        'DicomSeriesInstanceUID',
        'DicomSliceThickness',
        'DicomPixelSpacingX',
        'DicomPixelSpacingY'

    ],

    # Add:
    #   number of TRs
    #   duration of scan (how?) 
    #   make sure time is in correct time zone
    #   Echo Number
    #   Phase Encode Direction

    'MR' : [
        'DicomMagneticFieldStrength',
        'DicomSequenceName',
        'DicomRepetitionTime',
        'DicomEchoTime',
        'DicomEchoNumbers',
        'DicomFlipAngle',
        'DicomNumberOfAverages',
        'DicomAcquisitionNumber',
        'DicomSpacingBetweenSlices'
        ],

    # Tracer used
    # ReconstructionMethod
    # ScatterCorrectionMethod
    # Attenuation Correction Method
    # Isotope?
    'PT' : [
        'DicomReconstructionMethod',
        'DicomScatterCorrectionMethod',
        'DicomAttenuationCorrectionMethod',
        'DicomRadiopharmaceutical',
        'DicomRadionuclide'
    ]

}


# Return a string to include in the CVS file for a given column
def make_output_text(sess, fw_acq, fw_file, dcm, column):
    
    # Use dictionary for simple outputs
    action_dict = {
        'INDDID' : sess['indd_id'],
        'FlywheelSubjectID' : sess['subject_id'],
        'FlywheelSessionTimestampUTC' : sess['session_ts'],
        'FlywheelSessionInternalID' : fw_acq.parents.session,
        'FlywheelProjectInternalID' : fw_acq.parents.project,
        'FlywheelAcquisitionLabel' : fw_acq.label,
        'FlywheelSessionURL' : "https://upenn.flywheel.io/#/projects/%s/sessions/%s?tab=data" % 
            (fw_acq.parents.project,fw_acq.parents.session)
    } 

    val=None
    if column in action_dict:
        val=action_dict[column]

    elif column.startswith('FlywheelAcquisition'):
        key=column[len('FlywheelAcquisition'):]
        fcl = fw_file.get('classification', {});
        val = ';'.join(fcl[key]) if key in fcl else None

    elif column.startswith('DicomPixelSpacing'):
        spc= dcm.get('PixelSpacing');
        # spc_arr = map(lambda x:float(x.strip("[]' ")), spc_str.split(','))
        val = {
            'DicomPixelSpacingX' : spc[0],
            'DicomPixelSpacingY' : spc[1] 
        } [column] if spc is not None else None

    elif column == 'DicomRadiopharmaceutical':
        rpis=dcm.get('RadiopharmaceuticalInformationSequence',None)
        if rpis and len(rpis) > 0:
            val = rpis[0].get('Radiopharmaceutical',None)

    elif column == 'DicomRadionuclide':
        rpis=dcm.get('RadiopharmaceuticalInformationSequence',None)
        if rpis and len(rpis) > 0:
            rcs = rpis[0].get('RadionuclideCodeSequence', None)
            if rcs and len(rcs) > 0:
                val = rcs[0].get('CodeMeaning', None)
        
    elif column.startswith('Dicom'):
        val=dcm.get(column[5:], None)
    else:
        val=None

    return "%s" % val



# Get a listing of all the MRI scans across projects
def fw_list_acq(csv_stream, client, modality, project_path, col_list, subject=None, dicom_cache=None):

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

            # Check the subject against a name filter
            indd_id = fn_filter_inddid(sess.subject.label)

            # Print an error message
            if indd_id is None:
                logging.warning('Subject ID failed to match INDD filter: %s' % sess.subject.label)

            # Store the common information about this session
            # NOTE: the timestamp is in UTC 
            sess_cache[sess_id] = {
                'indd_id':indd_id,
                'subject_id':sess.subject.label,
                'session_ts':sess.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            }

        # Get the cached session
        S = sess_cache[sess_id]

        # Must have valid INDDID
        if S['indd_id'] is None:
            continue

        # Load the dicom for this acquisition, or skip this acq if not found
        dcm,f = fw_parse_acq_dicom(acq, dicom_cache)
        if dcm is None:
            logging.warning('Unable to extract DICOM for acquisition %s in subject %s session %s' %
                          (acq.label, S['subject_id'], S['session_ts']))
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
@click.option('--logfile', '-l', help='File for logging errors and warnings')
@click.option('--cache', '-c', help='Use a DICOM cache in the specified directory')
def get_indd_scans(key, subject, header, modality, logfile, cache):
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

    # Set up logging
    if logging:
        logging.basicConfig(filename=logfile, level=logging.DEBUG)

    # Set up cache directory
    if cache:
        if not os.path.exists(cache):
            os.makedirs(cache)

    for sp in search_paths:
        fw_list_acq(csv_stream, fw, mod_fw, sp, columns, subject, cache)











if __name__ == '__main__':
    get_indd_scans()
