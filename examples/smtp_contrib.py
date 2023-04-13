"""
Example
=======

Run as::
    $ Set-Variable LUIGI_CONFIG_PATH "path/to/config/luigi.cfg"
    $ luigi --module examples.smtp_contrib MyFlow --local-scheduler


"""
import luigi
from luigi.contrib.smtp import SmtpMail
from pathlib import Path
import os
from luigi.format import Nop
from dotenv import load_dotenv
from datetime import datetime


load_dotenv("local_testing.env", override=True)


class ExtraData(luigi.ExternalTask):
    attachment = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(path=self.attachment, format=Nop)


class MyFlow(SmtpMail):
    extra_attachments = ["test/_data/file1.txt"]
    bcc = os.getenv("MAIL_TEST_TO")
    subject = f'TEST EMAIL at {datetime.now().strftime("%H%M%S")}'

    def requires(self):
        attachs = [*Path("test/_data").rglob('*')]
        return [ExtraData(str(a)) for a in attachs[1:]]

    def output(self):
        ts = datetime.now().strftime('%Y%m%d%H%M%S')
        return luigi.LocalTarget(f"mail_{ts}.txt")


if __name__ == "__main__":
    luigi.run()
