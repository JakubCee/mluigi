"""
Dont forget to configure luigi.cfg sections like this:

[email]
format=html
method=msteams
receiver=some@email
sender=luigi-client-ra@host.com
##  force_send=True

[msteams]
webhook_url_success=<someUrl>
webhook_url_failure=<someUrl>

"""
import luigi
from luigi.contrib.notifiers import NotifiedTaskMixin

from dotenv import load_dotenv

load_dotenv('local_testing.env', override=True)


class TestNotifiedTask(NotifiedTaskMixin, luigi.Task):
     """Example to send message after successful run of this Task.
     """
     teams_message_text = "My custom message"
     teams_buttons = [("Button1", "https://gooogl.com"), ("Button1", "https://sharepoint.com")]

     def run(self):
          return 1

if __name__ == "__main__":
     t = TestNotifiedTask()
     luigi.build([t], local_scheduler=True)
