import warnings

from compliance.elements import node
import yaml
import dictdiffer
from pathlib import Path
from compliance.templates import formatter
from compliance.utils import functional


class Project(node.Node):
    def __init__(self, dataset, probe='first', protocol=None, export=False, metadataroot=None, **kwargs):
        super().__init__()
        self.dataset = dataset
        self.metadataroot = metadataroot
        if dataset.name is None:
            if not dataset.projects:
                self.id = dataset.projects[0]
        else:
            self.id = dataset.name

        self._construct_tree()
        self.report = None
        self.report_path = None
        self.protocol = None
        self.probe = probe
        try:
            if Path(protocol).exists():
                self.protocol = self.import_protocol(protocol)
        except FileNotFoundError:
            warnings.warn("Expected protocol reference not found on disk. Falling back to majority vote.")

        if export:
            self.export_protocol(protocol)

    def import_protocol(self, protopath):
        with open(protopath, 'r') as file:
            protocol = yaml.safe_load(file)
        return protocol

    def export_protocol(self, protopath):
        path = Path(protopath).parent
        filepath = path/'criteria_{0}.yaml'.format(functional.timestamp())
        with open(filepath, 'w') as file:
            yaml.dump(self.fparams, file, default_flow_style=False)

    def _construct_tree(self):
        for mode in self.dataset.modalities:
            modality = node.Node()
            modality.id = mode
            for sid in self.dataset.modalities[mode]:
                data = self.dataset[sid, mode]
                session_node = node.Node()
                session_node.id = sid
                d = None
                if len(data['files']) == 0:
                    warnings.warn("Expected > 3 .dcm files, Got 0.")
                    continue
                for f in data['files']:
                    d = node.Dicom(filepath=f)
                    session_node.insert(d)
                session_node.filepath = d.filepath.parent
                modality.insert(session_node)
            self.insert(modality)

    def diff(self, dcm_node, anchor):
        if anchor is None:
            return []
        diff = dictdiffer.diff(
            dict(dcm_node.fparams),
            dict(anchor.fparams)
        )
        return list(diff)

    def get_anchor(self, session, style='first'):
        if style == 'first':
            # A single .dcm file in the folder
            if len(session.children) == 1:
                anchor = session.children[0]
                success = anchor.load()
                if not success:
                    return None, -1
                return anchor, -1
            # Check until there are two dicom files, otherwise what is
            # the point choosing the single MRI file, as anchor
            for i in range(len(session.children)-1):
                anchor = session.children[i]
                if not anchor:
                    success = anchor.load()
                    # print(success, anchor.filepath)
                    if success:
                        return anchor, i
            return None, -1
        else:
            raise NotImplementedError("We support comparing from within the folder (generally the first file) only!")

    def post_order_traversal(self):
        for modality in self.children:
            for sess in modality.children:
                consistent_sess = True
                # Criteria for comparison, compare with first file, or use a protocol criteria
                anchor, i = self.get_anchor(sess)
                if anchor is None:
                    warnings.warn("All dicom files in folder {0} have a problem. "
                                  "What to do?".format(sess.children[0].filepath.parent), stacklevel=2)
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

    def partition_sessions_by_first(self):
        anchor = None
        for mode in self.children:
            consistent_sess = []
            inconsistent_sess = []
            i = 0
            for i, c in enumerate(mode.children):
                if c.consistent:
                    anchor = c
                    break
            for sub in mode.children[i:]:
                if sub.consistent:
                    sub.delta = self.diff(sub, anchor)
                    if not sub.delta:
                        consistent_sess.append(sub)
                    else:
                        inconsistent_sess.append(sub)
            mode.good_sessions = consistent_sess.copy()
            mode.bad_sessions = inconsistent_sess.copy()

    def partition_sessions_by_majority(self):
        pass

    def partition_sessions_by_reference(self):
        pass

    def check_compliance(self):
        # Generate complete report
        self.post_order_traversal()
        if self.probe == 'first':
            self.partition_sessions_by_first()
        elif self.probe == 'majority':
            self.partition_sessions_by_majority()
        elif self.probe == 'reference':
            self.partition_sessions_by_reference()
        else:
            # Generate a different type of report
            raise NotImplementedError("Report <style> not found.")
        pass

    def generate_report(self):
        formatter.HtmlFormatter(filepath=Path(self.metadataroot) / (
            '{0}_{1}.{2}'.format(self.dataset.name,
                                 functional.timestamp(),
                                 'html')
            ), params=self)
