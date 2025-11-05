import logging
import os

from dynaconf import Dynaconf

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["BASE_DIR"] = os.getenv("BASE_DIR", base_dir)
print(os.getenv("BASE_DIR", "world"))
print(base_dir)
app_settings = Dynaconf(root_path=f'{os.environ["BASE_DIR"]}/conf', settings_files=["*.toml"],
                    environments=True)

print(app_settings.to_dict())  # Prints all settings as a dictionary


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_gmail(subject: str, body: str):
    mail_usr = app_settings.mail_usr
    mail_pass = app_settings.mail_pass
    mail_to = app_settings.mail_to
    msg = MIMEMultipart()
    msg['From'] = mail_usr
    msg['To'] = ', '.join(mail_to)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.ehlo()
            server.starttls()
            server.login(mail_usr, mail_pass)
            server.sendmail(mail_usr, mail_to, msg.as_string())
        print("Email sent successfully.")
        logging.info("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")
        logging.error(f"Failed to send email: {e}")



