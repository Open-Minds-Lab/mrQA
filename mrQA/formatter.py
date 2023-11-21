import importlib
import smtplib
import ssl
from abc import ABC, abstractmethod
from email import encoders
from email.mime import base, multipart, text
from pathlib import Path

import jinja2

from mrQA import logger


class Formatter(ABC):
    def __init__(self):
        self.type_report = None
        self.sender_email = None
        self.receiver_email = None
        self.subject = None
        self.server = None
        self.port = None
        self.output = None
        self.params = None

    @abstractmethod
    def render(self):
        raise NotImplementedError("render implementation not found")


class BaseFormatter(Formatter):
    def __init__(self, filepath):
        super(BaseFormatter, self).__init__()
        # self.project = project
        self.filepath = filepath

    def _make_message(self):
        subject = "Dummy subject"
        body = "Regarding your project {0} for protocol mrQA" \
               "".format(self.params.name)

        message = multipart.MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = self.receiver_email
        message["Subject"] = subject

        message.attach(text.MIMEText(body, "plain"))
        with open(self.filepath, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this attachment
            part = base.MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment to part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={self.filepath}"
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        return message.as_string()

    def email(self, debug=True):
        if debug:
            # Use SMTP server
            server = 'localhost'
            port = 1025
        else:
            # Use Gmail server
            server = "smtp.gmail.com"
            port = 465

        password = input("Provide password: ")
        message = self._make_message()

        # Create a secure SSL context
        context = ssl.create_default_context()
        # Try to log in to server and send email
        try:
            server = smtplib.SMTP(server, port)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            if not debug:
                server.login(self.sender_email, password)
                server.sendmail(self.sender_email, self.receiver_email, message)
        except Exception as e:
            print(e)
        finally:
            server.quit()

    def render(self, *args, **kwargs):
        raise NotImplementedError


class HtmlFormatter(BaseFormatter):
    """
    Class to create an HTML report for compliance evaluation.

    Parameters
    ----------
    filepath : str
        Path to the html file to be created
    render : bool
        If True, the report is rendered immediately. Otherwise, the render
        method needs to be called explicitly.
    """

    def __init__(self, filepath, render=False):
        super(HtmlFormatter, self).__init__(filepath)
        self.template_folder = Path(__file__).resolve().parent
        self.hz_audit = None
        self.vt_audit = None
        self.plots = {}
        self.complete_ds = None

        self.skip_hz_report = False
        self.skip_vt_report = False
        self.skip_plots = True
        if render:
            self.render()

    def collect_hz_audit_results(self,
                                 compliant_ds,
                                 non_compliant_ds,
                                 undetermined_ds,
                                 subject_lists_by_seq,
                                 complete_ds,
                                 ref_protocol,
                                 **kwargs):
        """
        Collects results from horizontal audit and stores them. The
        dictionary is then passed to the jinja2 template for rendering.

        Parameters
        ----------
        compliant_ds : BaseDataset
            Dataset containing compliant sequences
        non_compliant_ds : BaseDataset
            Dataset containing non-compliant sequences
        undetermined_ds : BaseDataset
            Dataset containing sequences that could not be determined
        subject_lists_by_seq : dict
            Dictionary containing subject lists for each sequence
        complete_ds : BaseDataset
            Dataset containing all sequences
        ref_protocol : dict
            Reference protocol
        kwargs : dict
            Additional arguments to pass to the jinja2 template
        """
        if not complete_ds.get_sequence_ids():
            logger.error('No sequences found in dataset. Cannot generate'
                         'report')
            self.skip_hz_report = True
        if not ref_protocol:
            logger.error('Reference protocol is empty. Cannot generate'
                         ' report for horizontal audit.')
            self.skip_hz_report = True
        if not (compliant_ds.get_sequence_ids()
                or non_compliant_ds.get_sequence_ids()
                or undetermined_ds.get_sequence_ids()):
            logger.error('It seems the dataset has not been checked for '
                         'horizontal audit. Skipping horizontal audit report')
            self.skip_hz_report = True

        self.hz_audit = {
            'protocol': ref_protocol,
            'compliant_ds': compliant_ds,
            'non_compliant_ds': non_compliant_ds,
            'undetermined_ds': undetermined_ds,
            'sub_lists_by_seq': subject_lists_by_seq,
        }

        # add any additional kwargs to the hz_audit dict
        for key, value in kwargs.items():
            self.hz_audit[key] = value

        self.complete_ds = complete_ds

    def collect_vt_audit_results(self,
                                 compliant_ds,
                                 non_compliant_ds,
                                 sequence_pairs,
                                 complete_ds,
                                 parameters,
                                 **kwargs):
        """
        Collects results from horizontal audit and stores them. The
        dictionary is then passed to the jinja2 template for rendering.

        Parameters
        ----------
        compliant_ds : BaseDataset
            Dataset containing compliant sequences
        non_compliant_ds : BaseDataset
            Dataset containing non-compliant sequences
        complete_ds : BaseDataset
            Dataset containing all sequences
        sequence_pairs : list
         Sequence pairs compared for vertical audit. For ex.
         [('gre-field-mapping', 'rs-fMRI'), ('T1w', 'T2w')]
        parameters : list
            Parameters used for vertical audit.
            For ex. ['ShimSetting, 'FlipAngle']
        kwargs : dict
            Additional arguments to pass to the jinja2 template
        """

        if not complete_ds.get_sequence_ids():
            logger.error('No sequences found in dataset. Cannot generate'
                         'report')
            self.skip_vt_report = True
        if not (compliant_ds.get_sequence_ids()
                or non_compliant_ds.get_sequence_ids()):
            logger.error('It seems the dataset has not been checked for '
                         'vertical audit. Skipping vertical audit report')
            self.skip_vt_report = True

        self.vt_audit = {
            'complete_ds': complete_ds,
            'compliant_ds': compliant_ds,
            'non_compliant_ds': non_compliant_ds,
            'sequence_pairs': sequence_pairs,
            'parameters': parameters
        }

        # add any additional kwargs to the vt_audit dict
        for key, value in kwargs.items():
            self.vt_audit[key] = value

        self.complete_ds = complete_ds

    def collect_plots(self, **kwargs):
        for key, value in kwargs.items():
            self.plots[key] = value

        if not self.plots:
            logger.error('No plots found. Skipping plots section in report')
            self.skip_plots = True

    def render(self):
        """
        Renders the html report using jinja2 template. It will skip horizontal
        or vertical audit report if the corresponding audit was not performed.
        """
        if self.skip_hz_report and self.skip_vt_report:
            logger.error('Cannot generate report. See error log for details')
            return

        fs_loader = jinja2.FileSystemLoader(searchpath=self.template_folder)
        extn = ['jinja2.ext.loopcontrols']
        template_env = jinja2.Environment(loader=fs_loader, extensions=extn)

        template_file = "layout.html"
        template = template_env.get_template(template_file)

        output_text = template.render(
            hz=self.hz_audit,
            vt=self.vt_audit,
            plots=self.plots,
            skip_hz_report=self.skip_hz_report,
            skip_vt_report=self.skip_vt_report,
            skip_plots=self.skip_plots,
            complete_ds=self.complete_ds,
            imp0rt=importlib.import_module
        )
        f = open(self.filepath, 'w')
        f.write(output_text)
