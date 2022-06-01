import deepdiff

class Comparator:
    def __init__(self, config):
        pass

    def check_consistent(self, dcm_node):
        flag = True
        for key in self.protocol:
            if self.protocol[key]['enabled']:
                if self.protocol[key]['value'] != dcm_node[key]:
                    flag = False
                    self.fparams[key].append(dcm_node[key])
                self.fparams[key].append(dcm_node[key])

    def post_order_traversal(self):
        for sub in self.children:
            for sess in sub.children:
                for dcm_node in sess.children:
                    # If Dicom is already populated
                    if not dcm_node:
                        dcm_node.load()



    def check_compliance(self, span=True, style=None):
        # Generate complete report
        if span:
            self.post_order_traversal()
        else:
            # Generate a different type of report
            raise NotImplementedError("<span> has to be True.")
        pass


