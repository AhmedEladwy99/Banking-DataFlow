import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.logger import PipelineLogger
logger = PipelineLogger(__name__).get_logger()

class GmailNotifier:

    def __init__(self, credentials_path: str = "pipeline/config/email_credentials.txt"):
        try:
            with open(credentials_path, "r") as file:
                self.sender = file.readline().strip()
                self.password = file.readline().strip()
        except Exception as e:
            logger.error(f"Failed to load email credentials: {e}")
            raise
    
    def send_email(self, subject: str, message: str, to_email: str):
        try:
            msg = MIMEMultipart()
            msg["From"] = self.sender
            msg["To"] = to_email
            msg["Subject"] = subject
            msg.attach(MIMEText(message, "plain"))

            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.sendmail(self.sender, to_email, msg.as_string())

            logger.info(f"Email sent to {to_email}: {subject}")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")


if __name__ == "__main__":
    notifier = GmailNotifier()
    notifier.send_email(
        subject="Pipeline Notification",
        message="This is a test email from the GmailNotifier class.",
        to_email="mail@gmail.com"
    )

