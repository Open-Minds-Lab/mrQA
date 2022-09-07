 INPUT_DIR="/media/harsh/My Passport/MRI_Datasets/sinhah-20220520_201328/"
 OUTPUT_DIR="/home/harsh/My Passport/MRI_Datasets/metadata/"
 PROTOCOL_YAML="/home/harsh/PycharmProjects/compliance/compliance/delta/criteria.yaml"
 python /home/harsh/PycharmProjects/mrQA/mrQA/cli.py --dataroot "$INPUT_DIR" \
                                                               --metadataroot "$OUTPUT_DIR" \
                                                               --style xnat \
                                                               --name cha_mjff \
                                                               -c \
                                                               --protocol "$PROTOCOL_YAML"
