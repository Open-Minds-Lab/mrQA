from compliance.elements import node
import smtplib
import ssl
import yaml
from email.mime import base, multipart, text
from email import encoders


class Project(node.Node):
    def __init__(self, dataset, protopath):
        super().__init__()
        self.dataset = dataset
        self._construct_tree()
        self.sender_email = "mail.sinha.harsh@gmail.com"
        self.receiver_email = "harsh.sinha@pitt.edu"
        self.report = None
        self.report_path = None
        self.protocol = None
        self.export_protocol = True
        with open(protopath, 'r') as file:
            self.protocol = yaml.safe_load(file)

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

    def check_consistent(self, dcm_node):
        # for k, v in dcm_node:
        pass

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

    def export(self, astype='json', path=None, email=False):
        if astype == 'json':
            pass
        elif astype == 'html':
            pass
        elif astype == 'pdf':
            return

    def _make_message(self):
        subject = "Dummy subject"
        body = "This is an email attachment sent from OpenMinds regarding your project for protocol compliance"

        message = multipart.MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = self.receiver_email
        message["Subject"] = subject

        message.attach(text.MIMEText(body, "plain"))
        with open(self.report_path, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this attachment
            part = base.MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment to part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={self.report_path}"
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



