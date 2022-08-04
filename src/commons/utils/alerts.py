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
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, COMMASPACE, make_msgid

import time

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
        self.port = int(os.getenv("EMAIL_PORT") or 0)
        self.sender = sender
        self.receiver = receiver

    def send_mail(self, message):
        """
        Function to send mail using smtp server
        :param message: Email message
        """
        if self.mail_host and self.port:
            LOGGER.info("Sending mail alert...")
            with smtplib.SMTP(self.mail_host, self.port) as server:
                server.sendmail(self.sender, self.receiver.split(','), message.as_string())
        else:
            LOGGER.warning("Can't send mail as email host/port not found.")


# pylint: disable=too-many-instance-attributes
class MailNotification(threading.Thread):
    """This class contains common utility methods for Mail Notification."""

    def __init__(self, start_time, tp_id, **kwargs):
        """
        Init method:
        :param corio_start_time : Start time of the execution.
        :param tp_id : Test Plan ID to be sent in subject.
        :keyword sender: sender of mail.
        :keyword receiver: receiver of mail.
        :keyword health_check: Health check of cortx cluster.
        :keyword endpoint: S3 endpoint.
        """
        super().__init__()
        self.health_check = kwargs.get("health_check", False)
        self.sender = kwargs.get("sender", os.getenv("SENDER_MAIL_ID"))
        self.receiver = kwargs.get("receiver", os.getenv("RECEIVER_MAIL_ID"))
        self.event_fail = threading.Event()
        self.event_pass = threading.Event()
        self.event_abort = threading.Event()
        self.report_path = get_report_file_path(start_time)
        self._alert = bool(self.sender and self.receiver)
        self.mail_obj = Mail(sender=self.sender, receiver=self.receiver)
        self.health_obj = get_logical_node() if self.health_check else None
        self.host = self.health_obj.host if self.health_check else kwargs.get("endpoint")
        self.message_id = None
        self.tp_id = str(tp_id or '')

    def prepare_email(self, execution_status) -> MIMEMultipart:
        """
        Prepare email message with format and attachment
        :param execution_status: Execution status. In Progress/Fail
        :return: Formatted MIME message
        """
        # Mail common parameters.
        message = MIMEMultipart()
        message['From'] = self.sender
        message['To'] = COMMASPACE.join(self.receiver.split(','))
        message['Date'] = formatdate(localtime=True)
        # Message ID.
        if not self.message_id:
            self.message_id = make_msgid()
            message["Message-ID"] = self.message_id
        else:
            message["In-Reply-To"] = self.message_id
            message["References"] = self.message_id
        # Mail subject.
        subject = f"Corio TestPlan {str(self.tp_id or '')} is triggered on {self.host}"
        message['Subject'] = subject
        # Execution status
        body = f"Execution status: {execution_status}\n"
        # Cluster health and pod status.
        if self.health_check:
            hctl_status  = self.health_obj.get_hctl_status()[1]
            hctl_data = json.dumps(hctl_status, indent=4)
            result, pod_status = self.health_obj.execute_command(commands.CMD_POD_STATUS)
            health_status = "Cluster is healthy" if "offline" not in str(hctl_status) else \
                "Cluster is unhealthy"
            body += f"Cluster Health: {health_status}"
            storage_stat = hctl_status.get('filesystem', {'stats': ''}).get('stats')
            body += f"Storage: {storage_stat}"
            body += "PFA of hctl cluster status, pod status & execution status.\n"
            attachment = MIMEApplication(hctl_data, Name="hctl_status.txt")
            attachment['Content-Disposition'] = 'attachment; filename=hctl_status.txt'
            message.attach(attachment)
            if result:
                attachment = MIMEApplication(pod_status, Name="pod_status.txt")
                attachment['Content-Disposition'] = 'attachment; filename=pod_status.txt'
                message.attach(attachment)
            else:
                body += """Could not collect pod status"""
        # Corio execution report.
        if os.path.exists(self.report_path):
            with open(self.report_path, "rb") as fil:
                attachment = MIMEApplication(fil.read(), Name="execution_summery.txt")
            attachment['Content-Disposition'] = 'attachment; filename=execution_summery.txt'
            message.attach(attachment)
        else:
            body += f"Could not find {self.report_path}."
        # Jenkins execution url.
        build_url = os.getenv("BUILD_URL")
        if build_url:
            body += f"""Visit Jenkins Job: <a href="{build_url}">{build_url}</a>"""
        # Email body
        message.attach(MIMEText(body, "html", "utf-8"))
        return message

    def run(self):
        """Send Mail notification periodically."""
        message = None
        while not self.active_event():
            message = self.prepare_email(execution_status="In progress")
            self.mail_obj.send_mail(message)
            current_time = time.time()
            while time.time() < current_time + CORIO_CFG.email_interval_mins * 60:
                if self.active_event():
                    break
                time.sleep(60)
        if self.event_pass.is_set():
            message = self.prepare_email(execution_status="Passed")
        if self.event_fail.is_set():
            message = self.prepare_email(execution_status="Failed")
        if self.event_abort.is_set():
            message = self.prepare_email(execution_status="Aborted")
        self.mail_obj.send_mail(message)

    def active_event(self) -> bool:
        """Check the active event status."""
        return self.event_pass.is_set() or self.event_abort.is_set() or self.event_fail.is_set()


class SendMailNotification(MailNotification):
    """Send mail notification/status of execution."""

    def start_mail_notification(self):
        """Start the mail notification."""
        self.start()

    def send_failure_notification(self):
        """Send failure notification."""
        self.event_fail.set()
        self.join()

    def send_passed_notification(self):
        """Send passed notification."""
        self.event_pass.set()
        self.join()

    def send_aborted_notification(self):
        """Send aborted notifications."""
        self.event_abort.set()
        self.join()

    def email_alert(self, action, **kwargs):
        """Start the mail notifications."""
        if self._alert:
            if action == "start":
                self.start_mail_notification()
            elif action == "stop":
                if kwargs.get("tp"):
                    if kwargs.get("ids"):
                        self.send_failure_notification()
                    else:
                        self.send_aborted_notification()
                else:
                    self.send_passed_notification()
            else:
                LOGGER.warning("Incorrect action '%s' for mail alert...", action)
        else:
            LOGGER.warning("Can't send alert as sender: %s, receiver: %s not found.",
                           self.sender, self.receiver)
