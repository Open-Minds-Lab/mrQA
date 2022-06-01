from compliance.elements import node
import smtplib
import ssl
import yaml
from email.mime import base, multipart, text
from email import encoders
from pathlib import Path
import time


class Project(node.Node):
    def __init__(self, dataset, protopath, export=False):
        super().__init__()
        self.dataset = dataset
        self._construct_tree()
        self.sender_email = "mail.sinha.harsh@gmail.com"
        self.receiver_email = "harsh.sinha@pitt.edu"
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



