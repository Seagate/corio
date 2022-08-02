#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#
# -*- coding: utf-8 -*-
# !/usr/bin/python
"""
Module for generating email
"""
import json
import logging
import os
import smtplib
import threading
import time
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, COMMASPACE, make_msgid

from config import CORIO_CFG
from src.commons import commands
from src.commons.constants import ROOT
from src.commons.degrade_cluster import get_logical_node
from src.commons.utils.corio_utils import get_report_file_path

LOGGER = logging.getLogger(ROOT)


# pylint: disable=too-few-public-methods
class Mail:
    """Module to send mail"""

    def __init__(self, sender, receiver):
        """
        Init method
        :param sender: email address of sender
        :param receiver: email address of receiver
        """
        self.mail_host = os.getenv("EMAIL_HOST")
        self.port = int(os.getenv("EMAIL_PORT"))
        self.sender = sender
        self.receiver = receiver

    def send_mail(self, message):
        """
        Function to send mail using smtp server
        :param message: Email message
        """
        LOGGER.info("Sending mail with Subject %s:", message['Subject'])
        with smtplib.SMTP(self.mail_host, self.port) as server:
            server.sendmail(self.sender, self.receiver.split(','), message.as_string())


class MailNotification(threading.Thread):
    """This class contains common utility methods for Mail Notification."""

    def __init__(self, sender, receiver, tp_id, report_path):
        """
        Init method:
        :param sender: sender of mail
        :param receiver: receiver of mail
        :param tp_id : Test Plan ID to be sent in subject.
        :param report_path: Report file path.
        """
        super().__init__()
        self.event_fail = threading.Event()
        self.mail_obj = Mail(sender=sender, receiver=receiver)
        self.sender = sender
        self.receiver = receiver
        self.tp_id = tp_id
        self.health_obj = get_logical_node()
        self.report_path = report_path
        self.message_id = None

    def prepare_email(self, execution_status) -> MIMEMultipart:
        """
        Prepare email message with format and attachment
        :param execution_status: Execution status. In Progress/Fail
        :return: Formatted MIME message
        """
        hctl_status = json.dumps(self.health_obj.get_hctl_status()[1], indent=4)
        result, pod_status = self.health_obj.execute_command(commands.CMD_POD_STATUS)
        status = f"Corio TestPlan {str(self.tp_id or '')} is {execution_status} " \
                 f"on {self.health_obj.host}"
        subject = status
        body = f"<h3>{status}.</h2>\n" \
               f"<h3>PFA hctl cluster status, pod status & execution status.</h3>\n"
        build_url = os.getenv("BUILD_URL")
        if build_url:
            body += f"""Visit Jenkins Job: <a href="{build_url}">{build_url}</a>"""
        message = MIMEMultipart()
        message['From'] = self.sender
        message['To'] = COMMASPACE.join(self.receiver.split(','))
        message['Date'] = formatdate(localtime=True)
        message['Subject'] = subject
        if not self.message_id:
            self.message_id = make_msgid()
            message["Message-ID"] = self.message_id
        else:
            message["In-Reply-To"] = self.message_id
            message["References"] = self.message_id
        attachment = MIMEApplication(hctl_status, Name="hctl_status.txt")
        attachment['Content-Disposition'] = 'attachment; filename=hctl_status.txt'
        message.attach(attachment)
        if result:
            attachment = MIMEApplication(pod_status, Name="pod_status.txt")
            attachment['Content-Disposition'] = 'attachment; filename=pod_status.txt'
            message.attach(attachment)
        else:
            body += """<h3>Could not collect pod status</h3>"""
        name = os.path.basename(self.report_path)
        if os.path.exists(self.report_path):
            message.attach(MIMEText(body, "html"))
            with open(self.report_path, "rb") as fil:
                attachment = MIMEApplication(fil.read(), Name=name)
            attachment['Content-Disposition'] = f'attachment; filename={name}'
            message.attach(attachment)
        else:
            message.attach(MIMEText(body + f"<h3>Could not find {self.report_path}.</h3>", "html"))
        return message

    def run(self):
        """Send Mail notification periodically."""
        while not self.event_fail.is_set():
            message = self.prepare_email(execution_status="in progress")
            self.mail_obj.send_mail(message)
            current_time = time.time()
            while time.time() < current_time + CORIO_CFG.email_interval_mins * 60:
                if self.event_fail.is_set():
                    break
                time.sleep(60)
        message = self.prepare_email(execution_status="failed")
        self.mail_obj.send_mail(message)


def send_mail_notification(sender_email, receiver_email, tp_id, corio_start_time):
    """
    Send mail notification
    :param sender_email: Sender Mail ID
    :param receiver_email: Receiver Mail ID
    :param tp_id: Test Plan ID
    :param corio_start_time:
    :return MailNotification Process.
    """
    report_path = get_report_file_path(corio_start_time)
    mail_notify = MailNotification(sender=sender_email, receiver=receiver_email, tp_id=tp_id,
                                   report_path=report_path)
    mail_notify.start()
    return mail_notify
