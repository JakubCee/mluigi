import datetime
import logging
import luigi
import re
from luigi.notifications import msteams


logger = logging.getLogger('luigi-interface')


try:
    import pymsteams
except ImportError:
    logger.warning("Loading `pymsteams` module without required package `pymsteams`. "
                   "Will crash at runtime if MS Teams functionality is used.")


class MsTeamsNotification:
    def __init__(self, webhook_url: str, title: str, message: str, color: str = None):
        self.webhook_url = webhook_url
        self.title = title
        self.message = message
        self.teams_msg = pymsteams.connectorcard(self.webhook_url)
        self.teams_msg.title(title)
        self.teams_msg.text(message)
        self.section = pymsteams.cardsection()
        if color:
            self.teams_msg.color(color)

    def add_button(self, button_name, button_url):
        """Add button to the message"""
        if re.match("https?://", button_url):
            self.teams_msg.addLinkButton(button_name, button_url)
        else:
            logger.warning("Button url must start with `https://`")

    def add_section_keys(self, key, value):
        self.section.addFact(factname=key, factvalue=value)

    def send(self):
        self.section.disableMarkdown()
        self.teams_msg.addSection(self.section)
        self.teams_msg.send()


class NotifiedTaskMixin():
    # set optional text to show on task completion
    teams_message_text = None
    # button
    teams_buttons: list[tuple[str, str]] = None

    def __init__(self, *args, **kwargs):
        event_handler = super().event_handler

        @event_handler(luigi.Event.SUCCESS)
        def celebrate_success(self):
            """
            Report success event by sending info into MS Teams Channel for successful report generation.
            """
            config = msteams()
            message = self.teams_message_text or "Task finished with success :)"
            tm = MsTeamsNotification(webhook_url=config.webhook_url_success,
                                     title=self.get_task_family(),
                                     message=message,
                                     color="#4BB543")
            tm.add_section_keys("Finished at:", datetime.datetime.now().strftime("%Y-%B-%d %H:%M:%S"))
            if self.teams_buttons:
                for button_tup in self.teams_buttons:
                    tm.add_button(button_tup[0], button_tup[1])
            tm.send()




