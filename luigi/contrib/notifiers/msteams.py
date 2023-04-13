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
        """MsTeams notification for sending message via webhook. Supports title, body, cards and buttons.

        :param webhook_url: URL to MSTeams webhook
        :param title: Subjec of notification
        :param message: Main body od the notification
        :param color: HEX color to format message

        """
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
        """Add button to the message

        :param button_name: 
        :param button_url: 

        """
        if re.match("https?://", button_url):
            self.teams_msg.addLinkButton(button_name, button_url)
        else:
            logger.warning("Button url must start with `https://`")

    def add_section_keys(self, key, value):
        """Add key-value pairs into card section.

        :param key: Eg. "Executed by"
        :param value: Eg. "PC8473"

        """
        self.section.addFact(factname=key, factvalue=value)

    def send(self):
        """Send a message to webhook."""
        self.section.disableMarkdown()
        self.teams_msg.addSection(self.section)
        self.teams_msg.send()


class NotifiedTaskMixin():
    """Mix with other task (mixin first) to get notification on successful completion of mixed task.

    Configuration in `luigi.config` section [msteams], specify keys `webhook_url_success` and `webhook_url_failure`

    Attrs:
    teams_message_text: str = "Custom text to show in notification"
    teams_buttons: list[tuple[str, str]] =  [("Button1", "https://gooogl.com"), ("Button1", "https://sharepoint.com")]
    """
    # set optional text to show on task completion
    teams_message_text = ""
    # button
    teams_buttons: list[tuple[str, str]] = None
    # webhook_url - keep empty if use webhook from config file
    webhook_url = ""

    def __init__(self, *args, **kwargs):
        super().__init__()
        event_handler = super().event_handler

        @event_handler(luigi.Event.SUCCESS)
        def celebrate_success(self):
            """Report success event by sending info into MS Teams Channel for successful report generation."""
            config = msteams()
            message = self.teams_message_text or "Task finished with success :)"
            tm = MsTeamsNotification(webhook_url=self.webhook_url or config.webhook_url_success,
                                     title=self.get_task_family(),
                                     message=message,
                                     color="#4BB543")
            tm.add_section_keys("Finished at:", datetime.datetime.now().strftime("%Y-%B-%d %H:%M:%S"))
            if self.teams_buttons:
                for button_tup in self.teams_buttons:
                    tm.add_button(button_tup[0], button_tup[1])
            tm.send()




