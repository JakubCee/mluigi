"""
Example
=======

Run as::
    $ Set-Variable LUIGI_CONFIG_PATH "C:/apps/_PACKAGES/mluigi/luigi.cfg"
    $ luigi --module examples.smtp_contrib MyFlow --local-scheduler


"""
import luigi
from luigi.contrib.smtp import SmtpMail
from pathlib import Path
import os
from luigi.format import Nop
from dotenv import load_dotenv


load_dotenv("local_testing.env", override=True)


class ExtraData(luigi.ExternalTask):
   attachment = luigi.Parameter()
   def output(self):
       return luigi.LocalTarget(path=self.attachment, format=Nop)

class MyFlow(SmtpMail):
   extra_attachments = ["C:/apps/KRONOS_LOAD_SQL/EXCEL/QC_CONTRACTORS/KRONOS_QC_LINK_WRONG_BOOKINGS_CONTRACTORS.xlsx"]
   #host="mail.medtronic.com"
   #port=25

   def requires(self):
       attachs = [*Path("C:/apps/KRONOS_LOAD_SQL/EXCEL/QC_CONTRACTORS").rglob('*')]
       return [ExtraData(str(a)) for a in attachs[1:]]

   def output(self):
       return luigi.LocalTarget("mail.txt")



if __name__ == "__main__":
     luigi.run()