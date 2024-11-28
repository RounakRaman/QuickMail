from email import encoders
import imaplib
from email.mime.base import MIMEBase
from email import policy
from email.parser import BytesParser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import csv
import time
from email.message import EmailMessage
from email.utils import formataddr
import pandas as pd
import concurrent.futures
import time
from dotenv import load_dotenv
import os
import google.generativeai as genai 
import winsound 
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

load_dotenv()

# Sender's credentials
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
IMAP_SERVER = 'imap.gmail.com'
email_sender = "ramanrounak.work@gmail.com"
password_1 = os.getenv("PASSWORD")
API_KEY = os.getenv("API")
genai.configure(api_key=API_KEY)
name_sender="Rounak Raman (NSUT)"

OUTPUT_FILE = "correctdatabase.csv"

# Function to get relevant field
def getRelevantField(company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Changed model name to correct version
        response = model.generate_content(
            f"What is the relevant field in which the {company} is working? Mention only the prominent field in short",
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=12,
                temperature=1.0,
            )
        )
        return response.text
    except Exception as e:
        print(f"Error getting relevant field: {e}")
        return "technology and data solutions"  # fallback value

# Function to get subject
def getSubject(name, company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Changed model name
        response = model.generate_content(
            f"Please generate a formal and concise email subject line that addresses {name} and requests a referral for open positions at {company}.",
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=12,
                temperature=1.0,
            )
        )
        return response.text
    except Exception as e:
        print(f"Error getting subject: {e}")
        return f"Referral Request for {company}"  # fallback subject



def send_email(receiver_email, name, relevant_field, attachment_package, company):
    try:
        msg = EmailMessage()
        print("Hi")
        msg['Subject'] = getSubject(name, company)
        msg['From'] = formataddr((f"{name_sender}", email_sender))
        msg['To'] = receiver_email

        # Add a plain text version of the email
        plain_text = f"""
        Hi {name},

        I hope you're doing well! I'm Rounak Raman, a final-year student at NSUT doing my major in Information Technology with experience in data analysis, machine learning, and project management. My recent work includes developing predictive models for cognitive disability analysis at DRDO-INMAS and creating an air quality dashboard to reduce pollution levels in Delhi during my internship at Nation With Namo.

        I admire your company's focus on {relevant_field}. The innovative approaches your team employs in {relevant_field} resonate with my interests and career aspirations. I believe contributing to such impactful work would be a fantastic opportunity for growth and learning.

        With skills in Python, SQL, Power BI, and end-to-end ML pipelines along with product management skills, I am confident in my ability to add value to your team. My projects on global economic indicators and air quality analysis highlight my ability to solve real-world challenges through data-driven solutions.

        If you could kindly refer me for a relevant opportunity at your organization, I would be deeply grateful.

        Thank you for considering my request! I look forward to the chance to collaborate.

        Best regards,
        Rounak Raman
        +91-8826879389
        """


        html_content = f'''
        <html>
        <body>
            <p>Hi {name},</p>
            <p>I hope you're doing well! I'm <strong>Rounak Raman</strong>, a final-year student at NSUT doing my major in Information Technology with experience in data analysis, machine learning, and project management. My recent work includes developing predictive models for cognitive disability analysis at <strong>DRDO-INMAS</strong> and creating an air quality dashboard to reduce pollution levels in Delhi during my internship at <strong>Nation With Namo</strong>.</p>
            <p>I admire your company's focus on <strong>{relevant_field}</strong>. The innovative approaches your team employs in <strong>{relevant_field}</strong> resonate with my interests and career aspirations. I believe contributing to such impactful work would be a fantastic opportunity for growth and learning.</p>
            <p>With skills in <strong>Python, SQL, Power BI, and end-to-end ML pipelines</strong> along with product management skills, I am confident in my ability to add value to your team. My projects on global economic indicators and air quality analysis highlight my ability to solve real-world challenges through data-driven solutions.</p>
            <p>If you could kindly refer me for a relevant opportunity at your organization, I would be deeply grateful.</p>
            <p>Thank you for considering my request! I look forward to the chance to collaborate.</p>
            <p>Best regards,</p>
            <p><strong>Rounak Raman</strong></p>
            <p>+91-8826879389</p>
        </body>
        </html>
        '''

        print("Hola")
        msg.set_content(plain_text)
        msg.add_alternative(html_content, subtype="html")
        msg.add_attachment(attachment_package.get_payload(decode=True), maintype='application', subtype='octet-stream', filename=attachment_package.get_filename())

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as connection:
            connection.login(user=email_sender, password=password_1)
            connection.send_message(msg)

        
        print(f"Email successfully sent to {receiver_email}")
        winsound.Beep(1000, 1000)  # Frequency 1000Hz, duration 500ms
    except Exception as e:
        print(f"Error sending email: {e}")

def is_recent(sent_time, received_date, time_window=5):
    """
    Check if the bounce email timestamp is close to the sent email timestamp.

    Arguments:
    - sent_time: Datetime when the email was sent.
    - received_date: Datetime when the bounce email was received.
    - time_window: Allowed time difference in seconds (default is 300 seconds = 5 minutes).

    Returns:
    - True if the bounce is recent and matches the sent email, False otherwise.
    """
    try:
        received_datetime = parsedate_to_datetime(received_date)
        time_difference = abs((received_datetime - sent_time).total_seconds())
        return time_difference <= time_window  # Returns True if within allowed time window
    except Exception as e:
        print(f"Error parsing dates: {e}")
        return False

def check_for_bounce(to_address, sent_email_log):
    """
    Check if an email to a specific address bounced based on the subject line and timestamp.

    Arguments:
    - to_address: The recipient email address to check.
    - sent_email_log: A dictionary of sent emails with timestamps or IDs.

    Returns:
    - True if a bounce is detected for the given address, False otherwise.
    """
    try:
        with imaplib.IMAP4_SSL(IMAP_SERVER) as mail:
            mail.login(email_sender, password_1)
            mail.select('inbox')

            # Search for bounce emails
            status, data = mail.search(None, '(FROM "mailer-daemon" SUBJECT "Delivery Status Notification (Failure)")')
            if status != 'OK' or not data[0]:
                return False  # No bounce emails found
            
            for num in data[0].split():
                status, msg_data = mail.fetch(num, '(RFC822)')
                if status != 'OK':
                    continue  # Skip if fetching fails

                # Parse the email to extract the subject and timestamps
                msg = BytesParser(policy=policy.default).parsebytes(msg_data[0][1])
                subject = msg['Subject']
                received_date = msg['Date']  # Date the bounce was received

                # Correlate the bounce email with the sent email
                for sent_address, sent_time in sent_email_log.items():
                    if to_address == sent_address and is_recent(sent_time, received_date):
                        print(f"Bounced email detected for: {to_address}")
                        return True

        return False
    except Exception as e:
        print(f"Error checking bounce for {to_address}: {e}")
        return False


def process_row(row, attachment_package):
    
    try:
        print("Processing email for:", row["Name"])
        relevant_field = getRelevantField(row["Company name"])
        send_email(
            receiver_email=row["emails"],
            name=row["Name"],
            relevant_field=relevant_field,
            attachment_package=attachment_package,
            company=row["Company name"]
        )
        sent_email_log[row["emails"]] = datetime.now()  # Log the timestamp

        print("Waiting 1 second for delivery confirmation...")
        time.sleep(1)
        if not check_for_bounce(row["emails"]):             
                
                with open(OUTPUT_FILE, "a") as f:
                    f.write(
                        f"{row['Name']},{row['Company name']},{row['emails']}\n"
                    )
                print(f"Email validated for {row['emails']}")
        else:
                
                print(f"Email invalid: {row['emails']}")
    
    except Exception as e:
        print(f"Error processing row: {e}")

if __name__ == "__main__":
    try:
        sent_email_log = {}
        # Reading the CSV file
        df = pd.read_csv("data.csv")
        print(f"Loaded {len(df)} rows from CSV")

        # Reading the resume file
        filename = "Rounak_Raman_Resume.pdf"
        with open(filename, 'rb') as attachment:
            attachment_package = MIMEBase('application', 'octet-stream')
            attachment_package.set_payload(attachment.read())
            encoders.encode_base64(attachment_package)
            attachment_package.add_header('Content-Disposition', f"attachment; filename={filename}")

        BATCH_SIZE = 1
        x = 0

        while x < len(df):
            print(f"Processing batch starting at index {x}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                batch = df.iloc[x:x + BATCH_SIZE]
                futures = []
                for _, row in batch.iterrows():
                    future = executor.submit(process_row, row.to_dict(), attachment_package)
                    futures.append(future)
                
                # Wait for all futures to complete
                concurrent.futures.wait(futures)

            x += BATCH_SIZE
            print(f"Batch completed, waiting 60 seconds...")
            time.sleep(60)

    except Exception as e:
        print(f"Main process error: {e}")