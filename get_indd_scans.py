#!/bin/python
import click
import io
import flywheel
import pydicom
import re


# A list of search paths in FlyWheel used to search for INDD subjects.
# For now this is hard-coded
search_paths = ["cfn/PMC-CLINICAL", "dwolklab/NACC-SC"]


# This function reads dicom tags from the first acquisition
# in a FlyWheel session. Tags can then be accessed as simple
# attributes of the return object
def fw_parse_dicom(sess):
  
  # Get the first acquisition
  acq=sess.acquisitions.find_first()
  if acq is None:
    return None

  # Search for the first DICOM file
  try:
    f=next(x for x in acq.files if (lambda f: f.type=='dicom'))
    zi=acq.get_file_zip_info(f.name)
    fp=io.BytesIO(acq.read_file_zip_member(f.name, zi.members[0].path))
    return pydicom.dcmread(fp, stop_before_pixels=True)
  except:
    return None


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



# Get a listing of all the MRI scans across projects
def fw_list_mri(project, regex):

    for sess in project.sessions():

        # Check the subject against the regular expression
        if regex is not None and regex.match(sess.subject.label) is False:
            continue

        # Check that the session has acquisitions
        if fw_get_session_modality(sess) != 'MR':
            continue

        # Get the DICOM header of the first dicom file
        dcm = fw_parse_dicom(sess)

        # Parse all the acquisitions and for each print basic information
        for acq in sess.acquisitions():
            try:

                # Find the first dicom member of the acquisition
                f=next(x for x in acq.files if (lambda f: f.type=='dicom'))

                # Print the information about this acquisition in flat file
                print('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % 
                      (sess.subject.label,
                       sess.timestamp,
                       dcm.Modality,
                       dcm.MagneticFieldStrength,
                       dcm.InstitutionName,
                       acq.label,
                       f['classification']['Intent'][0],
                       f['classification']['Measurement'][0],
                       acq.uid,
                       acq.id))
            except:
                continue



# Get a listing of all the MRI scans across projects
def fw_list_acq(client, modality, project_path, subject=None):

    # Get a filter to search for all acquisitions of this type
    fw_filter = fw_make_acq_modality_filter(client, modality, project_path, subject)

    # If no filter, then the project or subject were not found
    if fw_filter is None:
        return

    # This dictionary associates sessions with common DICOM parameters. It avoids
    # having to download DICOM for every acquisition examined
    meta_cache={}

    # Use the filter to list acquisitions
    for acq in client.acquisitions.iter_find(fw_filter):

        # Get the session for this acquisition
        sess_id = acq.parents.session
        if sess_id not in meta_cache:

            # Load the session
            sess = client.get_session(sess_id)

            # Store the common information about this session
            meta_cache[sess_id] = {
                'subject_id':sess.subject.label,
                'session_ts':sess.timestamp,
                'dicom':fw_parse_dicom(sess)
            }

        # Find the first dicom member of the acquisition
        try:
            f=next(x for x in acq.files if (lambda f: f.type=='dicom'))
        except:
            continue

        # Print the row for this acquisition
        M = meta_cache[sess_id]
        dcm = M['dicom']
        fcl=f['classification']
        print('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % 
              (M['subject_id'],
               M['session_ts'],
               dcm.Modality,
               dcm.InstitutionName,
               dcm.MagneticFieldStrength if 'MagneticFieldStrength' in dcm else None,
               acq.label,
               fcl['Intent'][0] if 'Intent' in fcl else None,
               fcl['Measurement'][0] if 'Measurement' in fcl else None,
               fcl['Features'][0] if 'Features' in fcl else None,
               dcm.StudyInstanceUID,
               acq.parents.session,
               acq.parents.project))



# Main entrypoint
@click.command()
@click.option('--key', '-k', help='Path to the FlyWheel API key file', required=True)
@click.option('--subject', '-s', help='Only list information for one subject')
@click.option('--modality', '-m', type=click.Choice(['MRI', 'PET'], case_sensitive=False),
              help='Which modality to list', default='MRI')
def get_indd_scans(key, subject, modality):
    """Get a listing of INDD scans in FlyWheel in CSV format"""

    with open(key, 'r') as keyfile:
        fw_api_key = keyfile.read().strip()
    
    # Open connection to FW
    fw = flywheel.Client(fw_api_key)

    # Map the modality to flywheel lingo
    mod_map = { 'mri' : 'MR', 'pet' : 'PT' }

    for sp in search_paths:
        fw_list_acq(fw, mod_map[modality.lower()], sp, subject)











if __name__ == '__main__':
    get_indd_scans()
