PARAMETER_TAGS = {
    "manufacturer": [0x08, 0x70],
    "organ": [0x18, 0x15],
    "te": [0x18, 0x81],
    "tr": [0x18, 0x80],
    "b0": [0x18, 0x87],
    "flip_angle": [0x18, 0x1314],
    "bwpx": [0x18, 0x95],
    "echo_train_length": [0x18, 0x91],
    "comments": [0x20, 0x4000],
    "scanning_sequence": [0x18, 0x20],
    "sequence_variant": [0x18, 0x21],
    "mr_acquisition_type": [0x18, 0x23],
    "phase_encoding_lines": [0x18, 0x89],
    "bwp_phase_encode": [0x19, 0x1028],
    "phase_encoding_direction": [0x18, 0x1312],

}
HEADER_TAGS = {
    "image_header_info": [0x29, 0x1010],
    "series_header_info": [0x29, 0x1020],
    "series_description": [0x08, 0x103E],
    "series_number": [0x20, 0x11],
    "protocol_name": [0x18, 0x1030],
    "sequence_name": [0x18, 0x24]
}
SODict = {
    "1": "sequential",
    "2": "interleaved",
    "4": "singleshot"
}
SSDict = {
    "SE": "Spin Echo",
    "IR": "Inversion Recovery",
    "GR": "Gradient Recalled",
    "EP": "Echo Planar",
    "RM": "Research Mode"
}
SVDict = {
    "SK": "segmented k-space",
    "MTC": "magnetization transfer contrast",
    "SS": "steady state",
    "TRSS": "time reversed steady state",
    "SP": "spoiled",
    "MP": "MAG prepared",
    "OSP": "oversampling phase",
    "NONE": "no sequence variant"
}
ATDict = ["2D", "3D"]

