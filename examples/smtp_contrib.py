import luigi
from luigi.contrib.smtp import SmtpMail
from pathlib import Path
import os
from luigi.format import Nop

os.environ["LUIGI_CONFIG_PATH"] = "C:/apps/_PACKAGES/mluigi/luigi.cfg"

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