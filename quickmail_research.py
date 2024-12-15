import streamlit as st
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
from dotenv import load_dotenv
import os
import google.generativeai as genai
from datetime import datetime
import base64

# Constants
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
IMAP_SERVER = 'imap.gmail.com'
OUTPUT_FILE = "correctdatabase.csv"

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API")
genai.configure(api_key=API_KEY)

# Streamlit UI elements
st.title('Beyond Tech Promo Mail Sender')

# Email credentials
name_sender = st.text_input("Enter Sender's Name :")
email_sender = st.text_input('Enter Sender Email:')
password_1 = st.text_input('Enter Email Password:', type="password")
Mail_Content = st.text_area("Enter only the body of the mail here:") 
attachment_file = st.file_uploader('Upload your Resume/CV here: ', type=['pdf', 'docx', 'jpg'])

# Caching uploaded files and email databases
@st.cache_data
def load_file(uploaded_file):
    file_extension = uploaded_file.name.split('.')[-1].lower()
    if file_extension in ['xls', 'xlsx']:
        return pd.read_excel(uploaded_file)
    return pd.read_csv(uploaded_file)

@st.cache_data
def cache_attachment(file_data, file_name):
    attachment = MIMEBase('application', 'octet-stream')
    attachment.set_payload(file_data)
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', f'attachment; filename="{file_name}"')
    return attachment

# CSV File Upload
uploaded_file = st.file_uploader("Upload your Email database in Excel/CSV format: ", type=['xlsx', 'csv'])
if uploaded_file is not None:
    df = load_file(uploaded_file)
    st.write(df.head())  # Preview the content

# Function to get relevant field from GenAI
@st.cache_data
def getRelevantField(company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")
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
        return "technology and data solutions"

# Sanitize email headers
@st.cache_data
def sanitize_header(value):
    return value.replace("\n", "").replace("\r", "").strip()

# Function to send email
def send_email(receiver_email, name, relevant_field, attachment_package):
    try:
        msg = EmailMessage()
        msg['Subject'] = sanitize_header(f"{name}, are you Nervous About Placement Season? Don't worry we are here to help!")
        msg['From'] = sanitize_header(formataddr((name_sender, email_sender)))
        msg['To'] = sanitize_header(receiver_email)

        processed_content = Mail_Content.format(name=name, field=relevant_field)
        plain_text = f"Dear {name},\n\n{processed_content}\n\nBest Regards,\n{name_sender}\n"
        msg.set_content(plain_text)

        if attachment_package:
            msg.add_attachment(attachment_package.get_payload(decode=True), maintype='application', subtype='octet-stream', filename=attachment_package.get_filename())

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as connection:
            connection.login(user=email_sender, password=password_1)
            connection.send_message(msg)

        print(f"Email successfully sent to {receiver_email}")
    except Exception as e:
        print(f"Error sending email: {e}")

# Function to process each row in CSV and send emails
def process_row(row, attachment_package, sent_email_log):
    try:
        print("Processing email for:", row["Name"])
        relevant_field = getRelevantField(row["Company name"])
        send_email(
            receiver_email=row["emails"],
            name=row["Name"],
            relevant_field=relevant_field,
            attachment_package=attachment_package,
        )
        sent_email_log[row["emails"]] = datetime.now()  # Log the timestamp
        print("Email sent successfully to:", row["emails"])
    except Exception as e:
        print(f"Error processing row: {e}")

# Process button for sending emails
if st.button("Send Emails"):
    if uploaded_file and email_sender and password_1 and name_sender:
        sent_email_log = {}
        progress_container = st.empty()  # Reserve space for the progress bar
        percentage_container = st.empty()  # Reserve space for the percentage text
        progress_bar = progress_container.progress(0)  # Initialize the progress bar

        total_emails = len(df)
        emails_sent = 0

        # Handle attachment
        attachment_package = None
        if attachment_file is not None:
            attachment_package = cache_attachment(attachment_file.read(), f"{name_sender}_Resume.pdf")

        # Send emails in batches
        BATCH_SIZE = 10
        x = 0
        while x < total_emails:
            print(f"Processing batch starting at index {x}")
            batch = df.iloc[x:x + BATCH_SIZE]

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(process_row, row.to_dict(), attachment_package, sent_email_log)
                    for _, row in batch.iterrows()
                ]
                concurrent.futures.wait(futures)

            emails_sent += len(batch)
            progress_percentage = (emails_sent / total_emails) * 100
            progress_bar.progress(int(progress_percentage))
            percentage_container.write(f"Progress: {progress_percentage:.2f}%")

            x += BATCH_SIZE
            print("Batch completed, waiting 30 seconds...")
            time.sleep(30)

        st.success("Emails have been sent successfully.")
