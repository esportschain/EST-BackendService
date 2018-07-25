# -*- coding: utf-8 -*-

import smtplib

from email.mime.text import MIMEText

class SmtpEmail:

    def __init__(self, host, port, user, pwd, smtp_crypto=''):
        if smtp_crypto == 'tls':
            self.email = smtplib.SMTP()
            self.email.connect(host=host, port=port)
            self.email.starttls()
        elif smtp_crypto == 'ssl':
            self.email = smtplib.SMTP_SSL(host=host, port=port)
        else:
            self.email = smtplib.SMTP(host=host, port=port)
        self.email.login(user=user, password=pwd)

    def send(self, from_mail, to_mail, title, content):
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = from_mail
        msg['To'] = to_mail if type(to_mail) != list else ','.join(to_mail)
        msg['Subject'] = title
        try:
            self.email.sendmail(from_mail, to_mail, msg.as_string())
        except smtplib.SMTPException as e:
            print("Error: %s" % e)

    def quit(self):
        self.email.quit()

if __name__ == "__main__":
    e = SmtpEmail(host='', port=80, user='noreply@esportschain.org',
                  pwd='', smtp_crypto='')
    e.send(from_mail='noreply@esportschain.org', to_mail=['xx@xx.com'], title='',
           content='')