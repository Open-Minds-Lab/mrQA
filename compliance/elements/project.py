from compliance.elements import node
import yaml

from pathlib import Path
import time


class Project(node.Node):
    def __init__(self, dataset, protopath, export=False):
        super().__init__()
        self.dataset = dataset
        self.name = dataset.name
        self._construct_tree()
        self.report = None
        self.report_path = None
        self.protocol = None
        self.protocol = self.import_protocol(protopath)
        if export:
            self.export_protocol(protopath)

    def import_protocol(self, protopath):
        with open(protopath, 'r') as file:
            protocol = yaml.safe_load(file)
        return protocol

    def export_protocol(self, protopath):
        path = Path(protopath).parent
        time_string = time.strftime("%m_%d_%Y-%H_%M")
        filepath = path/'criteria_{0}.yaml'.format(time_string)
        with open(filepath, 'w') as file:
            yaml.dump(self.fparams, file, default_flow_style=False)

    def _construct_tree(self):
        for sid in self.dataset.subjects:
            sub = node.Node()
            for sess in self.dataset.sessions[sid]:
                data = self.dataset[sid, sess]
                session_node = node.Node()
                for f in data['files']:
                    d = node.Dicom(filepath=f)
                    session_node.insert(d)
                sub.insert(session_node)
            self.insert(sub)



