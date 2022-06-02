class DicomParsingError(Exception):
    """Custom error that is raised when some critical properties are not found in dicom file"""

    def __init__(self, message, *args, **kwargs):
        super().__init__(message)


class DicomPropertyNotFoundError(DicomParsingError):
    """Custom error that is raised when some critical properties are not found in dicom file"""

    def __init__(self, filepath, attribute):
        self.filepath = filepath
        self.attribute = attribute
        super().__init__("Expected {0} attribute not found in dicom file : {1}".format(attribute, filepath))

