import mimetypes
import os
import smtplib
from copy import copy
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path


import luigi
from luigi import Task
from luigi.task import Config
from luigi.parameter import Parameter, IntParameter, BoolParameter


class smtp_contrib(Config):
    host = Parameter(default=os.getenv("SMTP_HOST",""), description="Host for SMTP mail server")
    port = IntParameter(default=os.getenv("SMTP_PORT", 25), description="Port for SMTP mail server")
    local_hostname = Parameter(default="localhost", description="Local host name for SMTP server")
    from_ = Parameter(default="luigi@noreply.com", description="Sender email address")
    html = BoolParameter(default=True, description="Use html as message format")
    tls = BoolParameter(default=False, description="Use TLS")
    ssl = BoolParameter(default=False, description="Use SSL")
    username = Parameter(default=os.getenv("SMTP_USERNAME",""), description="Username login for SMTP server")
    password = Parameter(default=os.getenv("SMTP_PASSWORD",""), description="Password for SMTP server")




host = "mail.medtronic.com"
port = 25
to = "jakub.cehak@medtronic.com"
from_ = "luigi-no-reply@medtronic.com"
cc = to[:]
bcc = to[:]
subject = "Luigi Test"
html = True
message = """
<h1>Hello</h1>
<p>This is test</p>
"""


def list_to_commas(list_of_args):
    if isinstance(list_of_args, list):
        return ",".join(list_of_args)
    return list_of_args


class SmtpMail(Task):
    _cfg = smtp_contrib()

    host = _cfg.host
    port = _cfg.port
    from_=_cfg.from_
    html=_cfg.html
    message = message
    local_hostname = _cfg.local_hostname
    tls = _cfg.tls
    ssl = _cfg.ssl
    username = _cfg.username
    password = _cfg.password

    subject = "test luigi"
    to = 'jakub.cehak@medtronic.com'
    cc = None
    bcc = None
    extra_attachments = None

    def _connect_to_mailserver(self):
        self.smtp_server = smtplib.SMTP_SSL if self.ssl else smtplib.SMTP
        self.smtp_server = self.smtp_server(self.host, self.port, local_hostname=self.local_hostname)
        if self.tls and not self.ssl:
            self.smtp_server.ehlo()
            self.smtp_server.starttls()

        if self.username and self.password:
            self.smtp_server.login(self.username, self.password)

    @staticmethod
    def _get_mimetype(attachment):
        """Taken from https://docs.python.org/3/library/email.examples.html"""
        ctype, encoding = mimetypes.guess_type(str(attachment))
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        return maintype, subtype

    def _build_email(self):
        """
        Return Email object with specified args without attachments.
        """
        self.email = EmailMessage()
        self.email["To"] = self.to
        self.email["CC"] = self.cc
        self.email["Bcc"] = self.bcc
        self.email["From"] = self.from_
        self.email["Subject"] = self.subject
        self.email["Date"] = formatdate(localtime=True)
        content_type = "html" if self.html else "plain"
        self.email.add_alternative(self.message, subtype=content_type)
        return self.email

    def _add_attachment(self, attachment, email):
        """
        Shared function to add attachment to methods `_add_extra_attachments` and `input_attachments`.
        """
        attachment = Path(attachment)
        maintype, subtype = self._get_mimetype(attachment)
        email.add_attachment(
            attachment.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=attachment.name,
        )
        return email

    def _add_extra_attachments(self, attachments):
        """
        Attach extra attachment not coming from self.input().
        """
        for attachment in attachments:
            self._add_attachment(attachment=attachment, email=self.email)
            # attachment = Path(attachment)
            # maintype, subtype = self._get_mimetype(attachment)
            # self.email.add_attachment(
            #     attachment.read_bytes(),
            #     maintype=maintype,
            #     subtype=subtype,
            #     filename=attachment.name,
            # )
        return self.email

    def input_attachments(self):
        """Overwrite with `pass` if input should not be included."""
        for i in self.input():
            self._add_attachment(attachment=i.path, email=self.email)

    def run(self):
        """
        Connect, build, attach inputs and send.
        """
        self._connect_to_mailserver()
        self._build_email()
        mail_no_attachments = copy(self.email.as_string())
        # include extra attachment into email, not necessarily from luigi input()
        if self.extra_attachments:
            self._add_extra_attachments(self.extra_attachments)
        # include attachments from self.input(), if overwritten, do nothing, just send without attachment.
        self.input_attachments()
        self.smtp_server.send_message(self.email)
        with self.output().open('w') as f:
            f.write(str(mail_no_attachments))




