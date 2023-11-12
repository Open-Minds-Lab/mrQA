import pickle
from mrQA.project import plot_patterns

dict_ = pickle.load(open(
    '/home/sinhah/scan_data/vertical_abcd_mrqa_files/abcd-fmap-baseline-non-recommended_hz.adt.pkl', 'rb')
)

config_path = '/home/sinhah/github/mrQA/examples/mri-config-abcd.json'

plot_patterns(non_compliant_ds=dict_['non_compliant'],
              complete_ds=dict_['complete_ds'],
              config_path=config_path)
