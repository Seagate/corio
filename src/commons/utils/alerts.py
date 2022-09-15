#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#
"""Module for generating email."""

import json
import logging
import os
import smtplib
import threading
import time
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, COMMASPACE, make_msgid

from config import CORIO_CFG
from src.commons import commands
from src.commons.constants import ROOT
from src.commons.degrade_cluster import get_logical_node
from src.commons.utils.utility import get_report_file_path, convert_datetime_delta

LOGGER = logging.getLogger(ROOT)


# pylint: disable=too-few-public-methods
class Mail:
    """Module to send mail."""

    def __init__(self, sender, receiver):
        """
        Init method.

        :param sender: email address of sender
        :param receiver: email address of receiver
        """
        self.mail_host = os.getenv("EMAIL_HOST")
        self.port = int(os.getenv("EMAIL_PORT") or 0)
        self.sender = sender
        self.receiver = receiver

    def send_mail(self, message):
        """
        Send mail using smtp server.

        :param message: Email message
        """
        if self.mail_host and self.port:
            LOGGER.info("Sending mail alert...")
            with smtplib.SMTP(self.mail_host, self.port) as server:
                server.sendmail(self.sender, self.receiver.split(","), message.as_string())
        else:
            LOGGER.warning(
                "Can't send mail as email host: %s, port: %s not found.",
                self.mail_host,
                self.port,
            )


# pylint: disable=too-many-instance-attributes
class MailNotification(threading.Thread):
    """This class contains common utility methods for Mail Notification."""

    def __init__(self, start_time, tp_id, **kwargs):
        """
        Init method.

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
        self.start_time = start_time
        self.report_path = get_report_file_path(self.start_time)
        self._alert = bool(self.sender and self.receiver)
        self.mail_obj = Mail(sender=self.sender, receiver=self.receiver)
        self.health_obj = get_logical_node() if self.health_check else None
        self.host = self.health_obj.host if self.health_check else kwargs.get("endpoint")
        self.message_id = None
        self.tp_id = str(tp_id or "")

    def prepare_email(self, execution_status, status_code) -> MIMEMultipart:
        """
        Prepare email message with format and attachment.

        :param execution_status: Execution status. In Progress/Fail
        :param status_code: Color code used as per execution status.
        :return: Formatted MIME message
        """
        # Mail common parameters.
        execution_duration = convert_datetime_delta(datetime.now() - self.start_time)
        message = MIMEMultipart()
        message["From"] = self.sender
        message["To"] = COMMASPACE.join(self.receiver.split(","))
        message["Date"] = formatdate(localtime=True)
        # Message ID.
        if not self.message_id:
            self.message_id = make_msgid()
            message["Message-ID"] = self.message_id
        else:
            message["In-Reply-To"] = self.message_id
            message["References"] = self.message_id
        # Mail subject.
        message["Subject"] = f"Corio: TestPlan {str(self.tp_id or '')}, Server {self.host}"
        # Execution status
        body = "<h2 style='text-align:center;'>Corio Execution Status</h2>"
        body += f"""<table style='width: 100%; border: thin black dotted; text-align: left;'
         width = 'nowrap;'> <tr style='background-color:{status_code}'>"""
        body += f"""<td style='color: white; font-size: 124%; padding-left: 5px;'
         font-weight: bold; colspan=2><b>Execution status:</b> {execution_status}</td></tr>"""
        body += f"<tr><td><b>Execution started:</b></td> <td>{self.start_time}</td></tr>"
        body += f"<tr><td><b>Execution duration:</b></td> <td>{execution_duration}</td></tr>"
        # Cluster health and pod status.
        if self.health_check:
            hctl_status = self.health_obj.get_hctl_status()[1]
            health_status = (
                "Cluster is healthy"
                if self.health_obj.check_cluster_health()[1]
                else "Cluster is unhealthy"
            )
            body += f"<tr><td><b>Cluster Health:</b></td> <td>{health_status}</td></tr>"
            storage_stat = self.health_obj.check_cluster_storage()[1]
            body += f"<tr><td><b>Storage stat(bytes):</b></td> <td>{storage_stat}</td></tr>"
            attachment = MIMEApplication(json.dumps(hctl_status, indent=4), Name="hctl_status.txt")
            attachment["Content-Disposition"] = "attachment; filename=hctl_status.txt"
            message.attach(attachment)
            result, pod_status = self.health_obj.execute_command(commands.CMD_POD_STATUS)
            if result:
                attachment = MIMEApplication(pod_status, Name="pod_status.txt")
                attachment["Content-Disposition"] = "attachment; filename=pod_status.txt"
                message.attach(attachment)
            else:
                LOGGER.warning("Could not collect pod status.")
        # Corio execution report.
        if os.path.exists(self.report_path):
            with open(self.report_path, "rb") as fil:
                attachment = MIMEApplication(fil.read(), Name=os.path.basename(self.report_path))
            attachment["Content-Disposition"] = "attachment; filename=execution_summery_report.txt"
            message.attach(attachment)
        else:
            LOGGER.warning("Could not find %s", self.report_path)
        # Jenkins execution url.
        exec_url = os.getenv("BUILD_URL")
        if exec_url:
            body += (
                f"<tr><td><b>Visit Jenkins Job:</b></td> <td><a href="
                f"'{exec_url}'>build_url</a></td></tr>"
            )
        body += """<tr style='background-color:gray'><td style='color: white; font-size: 90%;
         padding-left: 5px; font-weight: bold;' colspan=2><b>Note: PFA of hctl, pod status of
         cluster & execution report.</b></td></tr>"""
        body += "</table>"
        # Email body
        message.attach(MIMEText(body, "html", "utf-8"))
        return message

    def run(self):
        """Send Mail notification periodically."""
        message = None
        while not self.active_event():
            message = self.prepare_email(execution_status="In Progress", status_code="#2B65EC")
            self.mail_obj.send_mail(message)
            current_time = time.time()
            while time.time() < current_time + CORIO_CFG.email_interval_mins * 60:
                if self.active_event():
                    break
                time.sleep(60)
        if self.event_pass.is_set():
            message = self.prepare_email(execution_status="Passed", status_code="#27AE60")
        if self.event_fail.is_set():
            message = self.prepare_email(execution_status="Failed", status_code="#E74C3C")
        if self.event_abort.is_set():
            message = self.prepare_email(execution_status="Aborted", status_code="#f4e242")
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
            LOGGER.warning(
                "Can't send alert as sender: %s, receiver: %s not found.",
                self.sender,
                self.receiver,
            )
