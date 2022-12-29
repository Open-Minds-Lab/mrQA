import smtplib
import ssl
from abc import ABC, abstractmethod
from email import encoders
from email.mime import base, multipart, text
from pathlib import Path
import importlib
import jinja2


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
    def __init__(self, filepath, params, render=True):
        super(HtmlFormatter, self).__init__(filepath)
        self.template_folder = Path(__file__).resolve().parent
        self.params = params
        if render:
            self.render()

    def render(self):
        """
        Render html page using jinja2
        :param
        :return:
        """
        fs_loader = jinja2.FileSystemLoader(searchpath=self.template_folder)
        extn = ['jinja2.ext.loopcontrols']
        template_env = jinja2.Environment(loader=fs_loader, extensions=extn)

        template_file = "templates/layout.html"
        template = template_env.get_template(template_file)

        output_text = template.render(
            dataset=self.params['ds'],
            subject_list=self.params['subject_list'],
            imp0rt=importlib.import_module
        )
        # self.output = weasyprint.HTML(string=output_text)
        f = open(self.filepath, 'w')
        f.write(output_text)


class PdfFormatter(HtmlFormatter):
    def __init__(self, filepath, params):
        super().__init__(filepath, params)
        # self.output = super(PdfFormatter, self).render(params)

    def render(self):
        return self.output.write_pdf(self.filepath)
