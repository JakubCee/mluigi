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
    """ """
    host = Parameter(default=os.getenv("SMTP_HOST",""), description="Host for SMTP mail server")
    port = IntParameter(default=os.getenv("SMTP_PORT", 25), description="Port for SMTP mail server")
    local_hostname = Parameter(default="localhost", description="Local host name for SMTP server")
    from_ = Parameter(default="luigi@noreply.com", description="Sender email address")
    html = BoolParameter(default=True, description="Use html as message format")
    tls = BoolParameter(default=False, description="Use TLS")
    ssl = BoolParameter(default=False, description="Use SSL")
    username = Parameter(default=os.getenv("SMTP_USERNAME",""), description="Username login for SMTP server")
    password = Parameter(default=os.getenv("SMTP_PASSWORD",""), description="Password for SMTP server")


def list_to_commas(list_of_args):
    """Convert list or tuple to comma-separated-string.

    Args:
      list_of_args: List to be transformed to comma-separated-string

    Returns:
      str

    """
    if isinstance(list_of_args, (list, tuple)):
        return ",".join(list_of_args)
    return list_of_args


class SmtpMail(Task):
    """Send email with attachments from input or add extra content. Recipients must be separated by semicolon.
    
    Examples
    
    class SendTestMail(SmtpMail):
        subject = "Test email"
        message = "<h2>Hello</h2>"
        html = True
        to = 'some.email@gmail.com;another.mail@gmail.com'
        bcc = 'some.email@gmail.com;another.mail@gmail.com'
        cc = 'some.email@gmail.com;another.mail@gmail.com'
        from_ = 'no-reply@gmail.com'
        extra_attachments = ["path/to/file.txt", "path/to/another/file2.txt"]

    Args:

    Returns:

    """
    _cfg = smtp_contrib()

    subject: str = "[Luigi]"
    message: str = ""
    to: str = ""
    cc: str = ""
    bcc: str = ""
    from_ = _cfg.from_
    extra_attachments: list = None

    host: str = _cfg.host
    port: int = _cfg.port
    local_hostname: str = _cfg.local_hostname
    html: bool = _cfg.html
    tls: bool = _cfg.tls
    ssl: bool = _cfg.ssl
    username: str = _cfg.username
    password: str = _cfg.password

    def _connect_to_mailserver(self):
        """ """
        self.smtp_server = smtplib.SMTP_SSL if self.ssl else smtplib.SMTP
        self.smtp_server = self.smtp_server(self.host, self.port, local_hostname=self.local_hostname)
        if self.tls and not self.ssl:
            self.smtp_server.ehlo()
            self.smtp_server.starttls()

        if self.username and self.password:
            self.smtp_server.login(self.username, self.password)

    @staticmethod
    def _get_mimetype(attachment):
        """Taken from https://docs.python.org/3/library/email.examples.html

        Args:
          attachment: 

        Returns:

        """
        ctype, encoding = mimetypes.guess_type(str(attachment))
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        return maintype, subtype

    def _resolve_recipients(self, as_list=False):
        """

        Args:
          as_list: Default value = False)

        Returns:

        """
        recipients = {
            "to": self.to.split(';') if self.to else [],
            "cc": self.cc.split(';') if self.cc else [],
            "bcc": self.bcc.split(';') if self.bcc else []
        }
        if as_list:
            l = []
            for i in recipients.values():
                l += i
            return l
        else:
            return recipients

    def _build_email(self):
        """ """
        recipients_dict = self._resolve_recipients(as_list=False)
        self.email = EmailMessage()
        self.email["To"] = recipients_dict['to']
        self.email["CC"] = recipients_dict['cc']
        self.email["From"] = self.from_
        self.email["Subject"] = self.subject
        self.email["Date"] = formatdate(localtime=True)
        content_type = "html" if self.html else "plain"
        self.email.add_alternative(self.message, subtype=content_type)
        return self.email

    def _add_attachment(self, attachment: str, email):
        """Shared function to add attachment to methods `_add_extra_attachments` and `input_attachments`.

        Args:
          attachment: str:
          email: 
          attachment: str: 

        Returns:

        """
        attachment = Path(attachment)
        if attachment.exists():
            maintype, subtype = self._get_mimetype(attachment)
            email.add_attachment(
                attachment.read_bytes(),
                maintype=maintype,
                subtype=subtype,
                filename=attachment.name,
            )
            return email

    def _add_extra_attachments(self, attachments: list[str]):
        """Attach extra attachment not coming from self.input().

        Args:
          attachments: list[str]:
          attachments: list[str]: 

        Returns:

        """
        for attachment in attachments:
            self._add_attachment(attachment=attachment, email=self.email)
        return self.email

    def input_attachments(self):
        """Overwrite with `pass` if input should not be included."""
        try:
            for i in self.input():
                self._add_attachment(attachment=i.path, email=self.email)
        except TypeError:
            self._add_attachment(attachment=self.input().path, email=self.email)

    def run(self):
        """Connect, build, attach inputs and send."""
        self._connect_to_mailserver()
        self._build_email()
        mail_no_attachments = copy(self.email.as_string())
        # include extra attachment into email, not necessarily from luigi input()
        if self.extra_attachments:
            self._add_extra_attachments(self.extra_attachments)
        # include attachments from self.input(), if overwritten, do nothing, just send without attachment.
        self.input_attachments()
        # resolve recipients, cc and bcc
        recipients_list = self._resolve_recipients(as_list=True)
        self.smtp_server.send_message(self.email, to_addrs=recipients_list)
        with self.output().open('w') as f:
            f.write(str(mail_no_attachments))




