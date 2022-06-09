import warnings

from compliance.elements import node
import yaml
import dictdiffer
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
                session_node.filepath = d.filepath.parent
                sub.insert(session_node)
            self.insert(sub)

    def diff(self, dcm_node, anchor):
        diff = dictdiffer.diff(
            dict(dcm_node.fparams),
            dict(anchor.fparams)
        )
        return list(diff)

    def get_anchor(self, session, style='first'):
        if style == 'first':
            # Check untill there are two dicom files, otherwise what is
            # the point choosing the single MRI file, as anchor
            for i in range(len(session.children)-1):
                anchor = session.children[i]
                if not anchor:
                    success = anchor.load()
                    if success:
                        return anchor, i
            return None, -1
        else:
            raise NotImplementedError("We support comparing from within the folder (generally the first file) only!")

    def post_order_traversal(self, style='first'):
        for sub in self.children:
            for sess in sub.children:
                consistent_sess = True
                # Criteria for comparison, compare with first file, or use a protocol criteria
                anchor, i = self.get_anchor(sess)
                if anchor is None:
                    warnings.warn("All dicom files in folder {0} have a problem. What to do?".format(sess.children[0].filepath.parent), stacklevel=2)
                    sess.error = True
                    continue
                for dcm_node in sess.children[i+1:]:
                    # If Dicom is already populated
                    if not dcm_node:
                        dcm_node.load()
                    dcm_node.delta = self.diff(dcm_node, anchor)
                    # If Dicom file has some differences, set consistent session to False
                    # Store delta in dicom object only, better
                    if dcm_node.delta:
                        consistent_sess = False

                # Given the fact that session is consistent
                # use one of the dicom files, to copy and store the consistent parameters
                # If session was inconsistent, make sure  you store the parameters used
                # for comparison
                sess.copy(anchor)
                sess.consistent = consistent_sess

    def partition_sessions(self):
        consistent_sess = []
        inconsistent_sess = []
        for sub in self.children:
            for sess in sub.children:
                if sess.consistent:
                    consistent_sess.append(sess)
                else:
                    inconsistent_sess.append(sess)
            return consistent_sess, inconsistent_sess

    def check_compliance(self, span=True, style=None):
        # Generate complete report
        if span:
            self.post_order_traversal()
        else:
            # Generate a different type of report
            raise NotImplementedError("<span> has to be True.")
        pass


