import pydicom
import warnings
from collections import defaultdict

from nibabel.nicom import csareader

from compliance.utils import config


class Node:
    def __init__(self):
        self.fparams = defaultdict(list)
        self.children = []
        self.verbose = False
        self.filepath = None
        self._nonzero = False

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

    def is

    def populate(self, *args, **kwargs):
        for k in self.children:
            if k.isconsistent():
                self.fparams.append(k.fparams)


class Dicom(Node):
    def __init__(self, filepath):
        super().__init__()
        self.filepath = filepath

    def free_memory(self):
        """
        Delete big files which are not required after extracting parameters
        :return:
        """
        del self.dicom
        del self.csaprops

    def load(self):
        self._read()
        self.set_property()
        self._csa_parser()
        self._adhoc_property()
        self._get_phase_encoding()
        self.free_memory()

    def _read(self):
        try:
            self.dicom = pydicom.dcmread(self.filepath)
        except OSError:
            print("Unable to read dicom file from disk.{0}".format(self.filepath))

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
            )
            return None

    def set_property(self):
        for k in config.PARAMETER_TAGS.keys():
            self[k] = self.get_value(k)

    def _csa_parser(self):
        self.image_header = csareader.read(self._get_header('ImageHeaderInfo'))
        self.series_header = csareader.read(self._get_header('SeriesHeaderInfo'))
        text = self.series_header['tags']['MrPhoenixProtocol']['items'][0].split("\n")
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
        self["MultiBandComment"] = self.get("Comments")
        so = str(eval(self.csaprops["sKSpace.ucMultiSliceMode"]))
        self["SliceOrder"] = config.SODict[so]
        if self.get("EchoTrainLength") > 1:
            check = (self.get("EchoTrainLength") == self.get("PhaseEncodingLines"))
            if not check:
                print("PhaseEncodingLines is not equal to EchoTrainLength : {0}".format(self.filepath))
        try:
            self['EffectiveEchoSpacing'] = 1000 / (
                    self.get('BandwidthPerPixelPhaseEncode') * self.get("PhaseEncodingLines"))
        except Exception as e:
            if self.verbose:
                if self.get('PhaseEncodingLines') is None:
                    warnings.warn('PhaseEncodingLines is None')
                else:
                    warnings.warn("Could not calculate EffectiveEchoSpacing : ")
            self['EffectiveEchoSpacing'] = None
        # three modes: warm-up, standard, advanced
        self["iPAT"] = self.csaprops.get("sPat.lAccelFactPE", None)
        self["ShimMethod"] = self.csaprops["sAdjData.uiAdjShimMode"]
        self["is3D"] = self.get("MRAcquisitionType") == '3D'

    def _get_phase_encoding(self, isFlipY=True):
        """
        https://github.com/rordenlab/dcm2niix/blob/23d087566a22edd4f50e4afe829143cb8f6e6720/console/nii_dicom_batch.cpp
        """
        is_skip = False
        if self.get('is3D'):
            is_skip = True
        if self.get('EchoTrainLength') > 1:
            is_skip = False
        phPos = self.image_header["tags"]['PhaseEncodingDirectionPositive']['items'].pop()
        ped_dcm = self.get_value("PedDCM")
        ped = ""
        assert ped_dcm in ["COL", "ROW"]
        if not is_skip and ped_dcm == "COL":
            ped = "j"
        elif not is_skip and ped_dcm == "ROW":
            ped = "i"
        if phPos >= 0 and not is_skip:
            if phPos == 0 and ped_dcm == 'ROW':
                ped += "-"
            elif ped_dcm == "COL" and phPos == 1 and isFlipY:
                ped += "-"
            elif ped_dcm == "COL" and phPos == 0 and not isFlipY:
                ped += "-"
            pedDict = {'i': 'Left-Right', 'i-': 'Right-Left',
                       'j-': 'Anterior-Posterior', 'j': 'Posterior-Anterior'}
            self["PhaseEncodingDirection"] = pedDict[ped]
        else:
            self["PhaseEncodingDirection"] = None

    def __str__(self):
        return self
