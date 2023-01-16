import mimetypes
import pathlib
import smtplib
import socket
from copy import copy
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from smtplib import SMTPAuthenticationError
from smtplib import SMTPSenderRefused
from smtplib import SMTPServerDisconnected

import luigi
from luigi import LocalTarget, Task

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
    host = "mail.medtronic.com"
    port = 25
    to = 'jakub.cehak@medtronic.com'
    subject = "test luigi"
    message = message
    extra_attachments=None
    html=True
    from_="no-reply@luigi.com"
    cc=None
    bcc=None
    local_hostname="localhost"
    tls=False
    ssl=False
    username=None
    password=None

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


if __name__ == "__main__":
    from pathlib import Path
    # attachs = [*Path("C:/apps/KRONOS_LOAD_SQL/EXCEL/QC_CONTRACTORS").rglob('*')]
    # mail = SmtpMail(host=host, port=port,to=to, subject=subject, message=message,extra_attachments=attachs[0], bcc=bcc, cc=cc)
    # mail.run()
    # # m = mail._build_email()
    # # m = mail._add_attachments(attachs)
    # # for i in m.items():
    # #     print(i)
    #

    from luigi.format import Nop
    class ExtraData(luigi.ExternalTask):

        attachment = luigi.Parameter()
        def output(self):
            return luigi.LocalTarget(path=self.attachment, format=Nop)

    class T(SmtpMail):
        extra_attachments = ["C:/apps/KRONOS_LOAD_SQL/EXCEL/QC_CONTRACTORS/READ ME.txt"]

        def requires(self):
            attachs = [*Path("C:/apps/KRONOS_LOAD_SQL/EXCEL/QC_CONTRACTORS").rglob('*')]
            return [ExtraData(str(a)) for a in attachs[1:]]

        def output(self):
            return luigi.LocalTarget("mail.txt")


    t = T()
    luigi.build([t], local_scheduler=True)

