from pathlib import Path
from nibabel.nicom.csareader import read

import json
import pydicom
import warnings


class DicomObject:
    def __init__(self, params=None):
        if not params:
            raise ValueError("Parameter file path required.")
        param_path = Path(__file__).resolve().parent.parent/params
        if not param_path.exists():
            raise ValueError("Invalid parameter file path.")

        with open(param_path, 'r') as f:
            self.params = json.load(f)

        self.fparams = {}
        self.dicom = None
        self.image_header = None
        self.series_header = None
        self.csaprops = None

    def parse(self, filepath):
        try:
            self.dicom = pydicom.dcmread(filepath)
            self.populate()
            return self
        except Exception:
            raise OSError("Unable to read/open file")

    def populate(self):
        self.csa_parser()
        self.set_property()
        self.get_phaseEncodingDir()

    def __eq__(self, other):
        if not isinstance(other, DicomObject):
            return NotImplemented
        if self.dicom.filename == other.dicom.filename:
            warnings.warn("Comparing same dicom files.")
        for k in self.fparams:
            if self.fparams[k] != other.fparams[k]:
                return False
        return True

    def isEqual(self, other, return_diff=True):
        flag = True
        diff = {}
        if not isinstance(other, DicomObject):
            return NotImplemented
        if self.dicom.filename == other.dicom.filename:
            warnings.warn("Comparing same dicom files.")
        for k in self.fparams:
            if self.fparams[k] != other.fparams[k]:
                flag = False
                diff[k] = [self.fparams[k], other.fparams[k]]
        if return_diff:
            return flag, str(diff)
        else:
            return flag

    def csa_parser(self):
        self.image_header = read(self.dicom[self.params['CSA']['ImageHeaderInfo']].value)
        self.series_header = read(self.dicom[self.params['CSA']['SeriesHeaderInfo']].value)
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
        self.fparams["iPAT"] = self.csaprops.get("sPat.lAccelFactPE", None)
        return

    def get_value(self, name):
        data = self.dicom.get(eval(self.params["PARAMETER_TAGS"][name]), None)
        if data:
            return data.value
        return None

    def get_property(self, name):
        """abstract method to retrieve a specific dicom property"""
        value = self.fparams.get(name, None)
        if value:
            return value
        else:
            warnings.warn(
                '{0} parameter at tag {1} does not exit in this DICOM file'.format(
                    name,
                    self.params['PARAMETER_TAGS'][name]
                )
            )
            return None

    def set_property(self):
        for k in self.params['PARAMETER_TAGS'].keys():
            self.fparams[k] = self.get_value(k)

        # imComment = self.get_value("Comments")
        # mb = [int(s[re.search(r"MB=", s.strip()).end() + 1:]) for s in
        #       imComment.split(";") if s if re.search(r"MB=", s.strip())]
        # assert len(mb) == 1
        # mb = mb[0]
        self.fparams["MultiBandComment"] = self.fparams["Comments"]
        mb = self.csaprops.get("sSliceAcceleration.lMultiBandFactor", None)
        self.fparams["MultiBand"] = mb
        so = str(eval(self.csaprops["sKSpace.ucMultiSliceMode"]))
        self.fparams["SliceOrder"] = self.params["SODict"][so]

        # root : https://neurostars.org/t/calculate-effective-echo-spacing-and-total-read-out-time-for-ge-machine/320
        # (SIEMENS)  https://lcni.uoregon.edu/kb-articles/kb-0003
        # BandwidthPerPixel

        if self.fparams["EchoTrainLength"] > 1:
            assert self.fparams["EchoTrainLength"] == self.fparams["PhaseEncodingLines"]
        # acquisition matrix
        # print(mr1[((0x18, 0x1310))], "Dimensions of the acquired frequency
        # /phase data before reconstruction. Multi-valued: frequency
        # rows\frequency columns\phase rows\phase columns.")

        # Effective Echo Spacing (ms) = 1000/(BPP * phase encoding lines)
        try:
            self.fparams['EffectiveEchoSpacing'] = 1000 / (
                    self.fparams['BandwidthPerPixelPhaseEncode'] * self.fparams["PhaseEncodingLines"])
        except:
            self.fparams['EffectiveEchoSpacing'] = None
        # three modes: warm-up, standard, advanced
        self.fparams["ShimMethod"] = eval(self.csaprops["sAdjData.uiAdjShimMode"])
        self.fparams["is3D"] = self.fparams["MRAcquisitionType"] == '3D'

    def get_phaseEncodingDir(self, isFlipY=True):
        """
        https://github.com/rordenlab/dcm2niix/blob/23d087566a22edd4f50e4afe829143cb8f6e6720/console/nii_dicom_batch.cpp
        """
        is_skip = False
        if self.fparams['is3D']:
            is_skip = True
        if self.fparams['EchoTrainLength'] > 1:
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
            self.fparams["PhaseEncodingDirection"] = pedDict[ped]
            # print("Phase Encoding Direction:", pedDict[ped])
            # return pedDict[ped]
        else:
            self.fparams["PhaseEncodingDirection"] = None

    def __str__(self):
        return self.fparams

