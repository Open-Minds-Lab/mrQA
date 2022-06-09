import pydicom
import warnings
from collections import defaultdict
from compliance.utils import functional
from compliance.utils import error
from nibabel.nicom import csareader
from pathlib import Path
from compliance.utils import config


class Node:
    def __init__(self):
        self.fparams = defaultdict()
        self.children = []
        self.verbose = False
        self.filepath = None
        self.consistent = False
        self.delta = None
        self.error = False

    def __bool__(self):
        return len(self) > 0

    def insert(self, other):
        self.children.append(other)

    # def __eq__(self, other):
    #     if not isinstance(other, Node):
    #         return NotImplemented
    #     if self.fparams is None:
    #         raise TypeError("Parameters expected, not NoneType for Node at index 0.")
    #     if other.fparams is None:
    #         raise TypeError("Parameters expected, not NoneType for Node at index 0.")
    #     flag = True
    #     diff = defaultdict(list)
    #     for k in self.fparams:
    #         if self.fparams[k] != other.fparams[k]:
    #             diff[k].append(self.fparams[k])
    #             diff[k].append(other.fparams[k])
    #             flag = False
    #     if not flag:
    #         print(diff)
    #     return flag

    def get(self, item):
        return self[item]

    def __getitem__(self, item):
        return self.fparams.get(item, None)

    def __setitem__(self, key, value):
        self.fparams[key] = value

    def __delitem__(self, key):
        del self.fparams[key]

    def __contains__(self, item):
        return item in self.fparams.keys()

    def __len__(self):
        return len(self.fparams)

    def __repr__(self):
        return repr(self.fparams)

    def copy(self, other):
        self.fparams = other.fparams.copy()

    def keys(self):
        return self.fparams.keys()

    def __str__(self):
        return str(self.fparams)



class Dicom(Node):
    def __init__(self, filepath):
        super().__init__()
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise OSError("Expected a valid filepath, Got invalid path : {0}\n"
                          "Consider re-indexing dataset.".format(filepath))

    def free_memory(self):
        """
        Delete big files which are not required after extracting parameters
        :return:
        """
        del self.dicom
        del self.csaprops

    def load(self):
        try:
            self._read()
            self.set_property()
            self._csa_parser()
            self._adhoc_property()
            self._get_phase_encoding()
            self.free_memory()
            return True
        except error.DicomParsingError:
            # Flush parameters for dicom file
            self.error_prone = True
            warnings.warn("Error parsing dicom file. Skip?", stacklevel=2)
            return False

    def _read(self):
        try:
            self.dicom = pydicom.dcmread(self.filepath)
        except OSError:
            raise error.DicomParsingError(
                "Unable to read dicom file from disk : {0}".format(self.filepath)
            )

    def get_value(self, name):
        data = self.dicom.get(config.PARAMETER_TAGS[name], None)
        if data:
            return data.value
        return None

    def _get_header(self, name):
        data = self.dicom.get(config.HEADER_TAGS[name], None)
        if data:
            return data.value
        return None

    def get_property(self, name):
        """abstract method to retrieve a specific dicom property"""
        value = self.get(name)
        if value:
            return value
        else:
            warnings.warn(
                '{0} parameter at tag {1} does not exit in this DICOM file'.format(
                    name,
                    config.PARAMETER_TAGS[name]
                )
                , stacklevel=2
            )
            return None

    def set_property(self):
        for k in config.PARAMETER_TAGS.keys():
            self[k] = self.get_value(k)

    def _csa_parser(self):
        self.image_header = csareader.read(self._get_header('image_header_info'))
        self.series_header = csareader.read(self._get_header('series_header_info'))
        items = functional.safe_get(self.series_header, 'tags.MrPhoenixProtocol.items')
        if items:
            text = items[0].split("\n")
        else:
            raise error.DicomPropertyNotFoundError(
                filepath=self.filepath,
                attribute='series_header.tags.MrPhoenixProtocol.items'
            )

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
        self.csaprops = props
        return

    def _adhoc_property(self):
        self["multi_band_comment"] = self.get("comments")
        so = self.csaprops["sKSpace.ucMultiSliceMode"]
        self["slice_order"] = config.SODict[so]
        # if self.get("echo_train_length") > 1: # Check if etl == pel
        #     check = (self.get("echo_train_length") == self.get("phase_encoding_lines"))
        #     if not check:
        #         print("PhaseEncodingLines is not equal to EchoTrainLength : {0}".format(self.filepath))
        if (self.get('bwp_phase_encode') is None) or (self.get('phase_encoding_lines') is None):
            self['effective_echo_spacing'] = None
        else:
            self['effective_echo_spacing'] = 1000 / (
                    self.get('bwp_phase_encode') * self.get("phase_encoding_lines"))
        # three modes: warm-up, standard, advanced
        self["ipat"] = self.csaprops.get("sPat.lAccelFactPE", None)
        self["shim_method"] = self.csaprops["sAdjData.uiAdjShimMode"]
        self["is3d"] = self.get("mr_acquisition_type") == '3D'

    def _get_phase_encoding(self, isflipy=True):
        """
        https://github.com/rordenlab/dcm2niix/blob/23d087566a22edd4f50e4afe829143cb8f6e6720/console/nii_dicom_batch.cpp
        """
        is_skip = False
        if self.get('is3D'):
            is_skip = True
        if self.get('echo_train_length') > 1:
            is_skip = False
        phpos = self.image_header["tags"]['PhaseEncodingDirectionPositive']['items'].pop()
        ped_dcm = self.get_value("phase_encoding_direction")
        ped = ""
        assert ped_dcm in ["COL", "ROW"]
        if not is_skip and ped_dcm == "COL":
            ped = "j"
        elif not is_skip and ped_dcm == "ROW":
            ped = "i"
        if phpos >= 0 and not is_skip:
            if phpos == 0 and ped_dcm == 'ROW':
                ped += "-"
            elif ped_dcm == "COL" and phpos == 1 and isflipy:
                ped += "-"
            elif ped_dcm == "COL" and phpos == 0 and not isflipy:
                ped += "-"
            ped_dict = {
                'i': 'Left-Right',
                'i-': 'Right-Left',
                'j-': 'Anterior-Posterior',
                'j': 'Posterior-Anterior'
            }
            self["phase_encoding_direction"] = ped_dict[ped]
        else:
            self["phase_encoding_direction"] = None
