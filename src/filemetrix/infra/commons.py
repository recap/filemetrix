import logging
import os
from dynaconf import Dynaconf
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["BASE_DIR"] = os.getenv("BASE_DIR", base_dir)
app_settings = Dynaconf(root_path=f'{os.environ["BASE_DIR"]}/conf', settings_files=["*.toml"],
                    environments=True)

print(app_settings.to_dict())  # Prints all settings as a dictionary
def _normalize_prefix(raw: str | None, default: str = "/api/v1") -> str:
    if not raw:
        return default
    p = raw.strip()
    if not p:
        return default
    # ensure single leading slash and no trailing slash (root stays "/")
    return "/" + p.strip("/")

# safe lookup from dynaconf settings, falling back to env var and default
API_PREFIX = getattr(app_settings, "API_PREFIX", None) or app_settings.get("API_PREFIX", None) or os.environ.get("API_PREFIX", None)
API_PREFIX = _normalize_prefix(API_PREFIX)


def send_mail(subject: str, body: str, to: list | None = None, from_addr: str | None = None) -> bool:
    mail_host = app_settings.get("mail_host", "smtp.gmail.com")
    mail_port = int(app_settings.get("mail_port", 587))
    mail_use_tls = bool(app_settings.get("mail_use_tls", True))
    mail_use_ssl = bool(app_settings.get("mail_use_ssl", False))
    mail_use_auth = bool(app_settings.get("mail_use_auth", False))

    mail_usr = app_settings.get("mail_usr", None)
    mail_pass = app_settings.get("mail_pass", None)

    mail_to = to if to is not None else app_settings.get("mail_to", [])
    if isinstance(mail_to, str):
        mail_to = [mail_to]
    if not mail_to:
        logging.error("No recipient specified for email.")
        return False

    from_addr = from_addr or app_settings.get("mail_from", mail_usr or "no-reply@example.com")

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(mail_to)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        if mail_use_ssl:
            with smtplib.SMTP_SSL(mail_host, mail_port, timeout=10) as server:
                if mail_use_auth and mail_usr and mail_pass:
                    server.login(mail_usr, mail_pass)
                server.sendmail(from_addr, mail_to, msg.as_string())
        else:
            with smtplib.SMTP(mail_host, mail_port, timeout=10) as server:
                server.ehlo()
                if mail_use_tls:
                    server.starttls()
                    server.ehlo()
                # Only attempt login if auth is requested and server supports AUTH
                if mail_use_auth and mail_usr and mail_pass:
                    if server.has_extn("auth"):
                        server.login(mail_usr, mail_pass)
                    else:
                        logging.warning("SMTP server does not advertise AUTH extension; skipping login.")
                server.sendmail(from_addr, mail_to, msg.as_string())

        logging.info("Email sent successfully.")
        return True
    except smtplib.SMTPAuthenticationError as e:
        logging.error("Authentication failed: %s", e)
        return False
    except Exception as e:
        logging.error("Failed to send email: %s", e)
        return False


