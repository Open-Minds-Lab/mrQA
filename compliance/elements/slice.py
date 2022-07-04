from pathlib import Path
from compliance.utils import config, functional
import pydicom
import warnings
from nibabel.nicom import csareader
from collections import defaultdict


class Node:
    def __init__(self, path):
        self.path = path
        self.consistent = False
        self.name = None
        self.error = False
        self.children = []
        self.params = defaultdict()

        self.delta = None
        self.good_children = []
        self.bad_children = []

    def insert(self, other):
        self.children.append(other)


def parse(dicom_path):
    filepath = Path(dicom_path)
    if not filepath.exists():
        raise OSError("Expected a valid filepath, Got invalid path : {0}\n"
                      "Consider re-indexing dataset.".format(filepath))

    try:
        dicom = pydicom.dcmread(filepath,
                                stop_before_pixels=True)
    except OSError:
        raise FileNotFoundError(
            "Unable to read dicom file from disk : {0}".format(filepath)
        )
    node = Node(filepath)
    for k in config.PARAMETER_TAGS.keys():
        value = get_value(dicom, k)
        # the value should be hashable
        # a dictionary will be used later to count the majority value
        if not functional.is_hashable(value):
            value = str(value)
        node.params[k] = value

    csa_values = csa_parser(dicom)
    node.params["slice_order"] = config.SODict[csa_values['so']]
    node.params['ipat'] = csa_values['ipat']
    node.params['shim'] = csa_values['shim']

    node.params["is3d"] = get_value(dicom, "mr_acquisition_type") == '3D'
    node.params["modality"] = "_".join([
        str(get_header(dicom,  "series_number")),
        get_header(dicom, "series_description")]).replace(" ", "_")
    node.params["effective_echo_spacing"] = effective_echo_spacing(dicom)
    node.params["phase_encoding_direction"] = get_phase_encoding(dicom,
                                                                 is3d=node.params['is3d'],
                                                                 echo_train_length=node.params['echo_train_length'])
    return node


def get_value(dicom, name):
    data = dicom.get(config.PARAMETER_TAGS[name], None)
    if data:
        return data.value
    return None


def get_header(dicom, name):
    data = dicom.get(config.HEADER_TAGS[name], None)
    if data:
        return data.value
    return None


def csa_parser(dicom):
    series_header = csareader.read(get_header(dicom, 'series_header_info'))
    items = functional.safe_get(series_header, 'tags.MrPhoenixProtocol.items')
    if items:
        text = items[0].split("\n")
    else:
        raise FileNotFoundError

    start = False
    end = False
    props = {}
    for e in text:
        if e[:15] == '### ASCCONV END':
            end = True
        if start and not end:
            ele = e.split()
            if ele[1].strip() == "=":
                props[ele[0]] = ele[2]
        if e[:17] == '### ASCCONV BEGIN':
            start = True

    so = props.get("sKSpace.ucMultiSliceMode", None)
    ipat = props.get("sPat.lAccelFactPE", None)
    shim = props.get("sAdjData.uiAdjShimMode", None)
    return {
        'so': so,
        'ipat': ipat,
        'shim': shim
        }


def effective_echo_spacing(dicom):
    # if self.get("echo_train_length") > 1: # Check if etl == pel
    #     check = (self.get("echo_train_length") == self.get("phase_encoding_lines"))
    #     if not check:
    #         print("PhaseEncodingLines is not equal to EchoTrainLength : {0}".format(self.filepath))
    bwp_phase_encode = get_value(dicom, 'bwp_phase_encode')
    phase_encoding = get_value(dicom, 'phase_encoding_lines')

    if (bwp_phase_encode is None) or (phase_encoding is None):
        return None
    else:
        value = 1000 / (
                bwp_phase_encode * phase_encoding)

        # Match value to output of dcm2niix
        return value/1000


def get_phase_encoding(dicom, is3d, echo_train_length, is_flipy=True):
    """
    https://github.com/rordenlab/dcm2niix/blob/23d087566a22edd4f50e4afe829143cb8f6e6720/console/nii_dicom_batch.cpp
    """
    is_skip = False
    if is3d:
        is_skip = True
    if echo_train_length > 1:
        is_skip = False
    image_header = get_header(dicom, 'image_header_info')
    phvalue = functional.safe_get(image_header, 'tags.PhaseEncodingDirectionPositive.items')
    if phvalue:
        phpos = phvalue[0]
    else:
        return None

    ped_dcm = get_value(dicom, "phase_encoding_direction")

    ped = ""
    assert ped_dcm in ["COL", "ROW"]
    if not is_skip and ped_dcm == "COL":
        ped = "j"
    elif not is_skip and ped_dcm == "ROW":
        ped = "i"
    if phpos >= 0 and not is_skip:
        if phpos == 0 and ped_dcm == 'ROW':
            ped += "-"
        elif ped_dcm == "COL" and phpos == 1 and is_flipy:
            ped += "-"
        elif ped_dcm == "COL" and phpos == 0 and not is_flipy:
            ped += "-"
        ped_dict = {
            'i': 'Left-Right',
            'i-': 'Right-Left',
            'j-': 'Anterior-Posterior',
            'j': 'Posterior-Anterior'
        }
        return ped_dict[ped]
    else:
        return None
