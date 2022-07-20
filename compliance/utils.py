import time
from collections import Counter, defaultdict
from MRdataset.utils import is_hashable


def timestamp():
    time_string = time.strftime("%m_%d_%Y_%H_%M")
    return time_string


def majority_attribute_values(iterable, missing=None):
    counts = {}
    categories = set(counts)
    for length, element in enumerate(iterable):
        categories.update(element)
        for cat in categories:
            try:
                counter = counts[cat]
            except KeyError:
                counts[cat] = counter = Counter({missing: 0})
            value = element.get(cat, missing)
            if not is_hashable(value):
                value = str(value)
            counter[value] += 1
    params = {}
    for k in counts.keys():
        params[k] = counts[k].most_common(1)[0][0]
    return params

# try:
#     if Path(reference_path).exists():
#         self.reference_path= self.import_protocol(reference_path)
# except FileNotFoundError:
#     warnings.warn("Expected protocol reference not found on disk.
#     Falling back to majority vote.")
#
# if export:
#     self.export_protocol(reference_path)

# def import_protocol(self, protopath):
#     """"""
#
#     with open(protopath, 'r') as file:
#         protocol = yaml.safe_load(file)
#     return protocol

# def export_protocol(self, protopath):
#     path = Path(protopath).parent
#     filepath = path / 'criteria_{0}.yaml'.format(functional.timestamp())
#     with open(filepath, 'w') as file:
#         yaml.dump(self.fparams, file, default_flow_style=False)

# def check_compliance(self):
#     # Generate complete report
#     self.post_order_traversal()
#     if self.strategy == 'first':
#         self.partition_sessions_by_first()
#     elif self.strategy == 'majority':
#         self.partition_sessions_by_majority()
#     elif self.strategy == 'reference':
#         self.partition_sessions_by_reference()
#     else:
#         # Generate a different type of report
#         raise NotImplementedError("Report <style> not found.")
#     pass


def extract_reasons(data):
    return list(zip(*data))[1]
