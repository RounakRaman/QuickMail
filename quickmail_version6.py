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
import time
from dotenv import load_dotenv
import os
import google.generativeai as genai 
#import winsound 
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import re 
import json 

if "current_index" not in st.session_state:
    st.session_state.current_index = 0  # Tracks the current index in the email list
if "is_running" not in st.session_state:
    st.session_state.is_running = False  # Default to not running
    
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
IMAP_SERVER = 'imap.gmail.com'

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API")
genai.configure(api_key=API_KEY)

# Streamlit UI elements
st.title('QuickMail Email Sending Tool')

# Email credentials
name_sender = st.text_input("Enter Sender's Name :")
email_sender = st.text_input('Enter Sender Email:')
password_1 = st.text_input('Enter Email Password:', type="password")
Mail_Content = st.text_area("Enter only the body of the mail here:") 
attachment_file = st.file_uploader('Upload your Resume/CV here: ', type=['pdf', 'docx', 'jpg'])

OUTPUT_FILE = "correctdatabase.csv"

# CSV File Upload
uploaded_file = st.file_uploader("Upload your Email database in CSV format: ", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Data Preview", df.head())

# Function to get subject and relevant field
def getRelevantField(company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Using the correct model
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

def getSubject(name, company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Using the correct model
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
        return f"Referral Request for {company}"

# Function to send email
def send_email(receiver_email, name, relevant_field, attachment_package, company):
    try:
        msg = EmailMessage()
        msg['Subject'] = getSubject(name, company)
        msg['From'] = formataddr((f"{name_sender}", email_sender))
        msg['To'] = receiver_email
        processed_content = Mail_Content.format(name=name, company=company, field=relevant_field)
        plain_text = f"Hi {name}\n\n{processed_content}\n\nBest Regards,\n{name_sender}\n"


        # html_content = f'''
        # <html>
        # <body>
        #     <p>Hi {name},</p>
        #     <p>I hope you're doing well! I'm <strong>Rounak Raman</strong>, a final-year student at NSUT doing my major in Information Technology with experience in data analysis, machine learning, and project management. My recent work includes developing predictive models for cognitive disability analysis at <strong>DRDO-INMAS</strong> and creating an air quality dashboard to reduce pollution levels in Delhi during my internship at <strong>Nation With Namo</strong>.</p>
        #     <p>I admire your company's focus on <strong>{relevant_field}</strong>. The innovative approaches your team employs in <strong>{relevant_field}</strong> resonate with my interests and career aspirations. I believe contributing to such impactful work would be a fantastic opportunity for growth and learning.</p>
        #     <p>With skills in <strong>Python, SQL, Power BI, and end-to-end ML pipelines</strong> along with product management skills, I am confident in my ability to add value to your team. My projects on global economic indicators and air quality analysis highlight my ability to solve real-world challenges through data-driven solutions.</p>
        #     <p>If you could kindly refer me for a relevant opportunity at your organization, I would be deeply grateful.</p>
        #     <p>Thank you for considering my request! I look forward to the chance to collaborate.</p>
        #     <p>Best regards,</p>
        #     <p><strong>Rounak Raman</strong></p>
        #     <p>+91-8826879389</p>
        # </body>
        # </html>
        # '''
        
        msg.set_content(plain_text)
        #msg.add_alternative(html_content, subtype="html")
        if attachment_package:
            msg.add_attachment(attachment_package.get_payload(decode=True), maintype='application', subtype='octet-stream', filename=attachment_package.get_filename())

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as connection:
            connection.login(user=email_sender, password=password_1)
            connection.send_message(msg)

        print(f"Email successfully sent to {receiver_email}")
#        winsound.Beep(1000, 1000)  # Frequency 1000Hz, duration 500ms
    except Exception as e:
        print(f"Error sending email: {e}")

def check_for_bounce(to_address, sent_email_log):
    """
    Check if an email to a specific address bounced based on the subject line and body.

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
                print("kyu hai bhai tu")
                return False  # No bounce emails found

            for num in data[0].split():
                status, msg_data = mail.fetch(num, '(RFC822)')
                if status != 'OK':
                    print("kaisa hai bhai")
                    continue  # Skip if fetching fails

                # Parse the email to extract the subject, date, and body
                msg = BytesParser(policy=policy.default).parsebytes(msg_data[0][1])
                subject = msg['Subject']
                received_date = msg['Date']
                body = msg.get_body(preferencelist=('plain')).get_payload(decode=True).decode()

                # Check if the bounce contains the address and invalid message
                if to_address in body and re.search(r"550 5\.1\.1", body):
                    print("aara bhai")
                    print(f"Bounced email detected for: {to_address}")
                    return True
                if to_address in body and re.search(r"NXDOMAIN", body):
                    print(f"Bounced email detected for DNS error (NXDOMAIN): {to_address}")
                    return True
                if to_address in body and re.search(r"550 5\.7\.1", body):
                    print(f"Bounced email detected for 550 5.7.1 (message rejected): {to_address}")
                    return True
                if to_address in body and re.search(r"550 5\.1\.0", body):
                    print(f"Bounced email detected for unknown recipient: {to_address}")
                    return True
                if to_address in body and re.search(r"550 5\.1\.2", body):
                    print(f"Bounced email detected for mailbox not found: {to_address}")
                    return True
                if to_address in body and re.search(r"550 5\.1\.3", body):
                    print(f"Bounced email detected for unavailable mailbox: {to_address}")
                    return True

        return False
    except Exception as e:
        print(f"Error checking bounce for {to_address}: {e}")
        return False        

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
            company=row["Company name"]
        )
        sent_email_log[row["emails"]] = datetime.now()  # Log the timestamp

        print("Waiting 1 second for delivery confirmation...")
        time.sleep(1)
        st.write(f"Email sent successfully to: {row['emails']}")

        if not check_for_bounce(row["emails"], sent_email_log):  # Check bounce using sent_email_log
            output_data.append({"Name": row["Name"], "Company name": row["Company name"], "emails": row["emails"]})
            
            with open(OUTPUT_FILE, "a") as f:
                f.write(f"{row['Name']},{row['Company name']},{row['emails']}\n")
            print(f"Email validated for {row['emails']}")
        else:
            print(f"Email invalid: {row['emails']}")

    except Exception as e:
        print(f"Error processing row: {e}")
        st.write(f"Error processing email for {row['emails']}: {e}")


 
        

# Output list to collect valid emails
output_data = []

# Process button for sending emails
# Process button for sending emails
if not st.session_state.is_running:
    if st.button("Send Emails"):
        st.session_state.is_running = True  # Mark as running
        # Start processing emails here
        st.write("Processing emails...")




# Email sending logic
    if st.session_state.is_running and uploaded_file and email_sender and password_1 and name_sender:
        sent_email_log = {}
        print("Hola")
        # Handling the attachment
        if attachment_file is not None:
            filename = f"{name_sender}_Resume.pdf"
            attachment_package = MIMEBase('application', 'octet-stream')
            attachment_package.set_payload(attachment_file.read())
            encoders.encode_base64(attachment_package)
            attachment_package.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
        else:
            attachment_package = None  # No attachment if the file is not uploaded

        BATCH_SIZE = 1

        # Start processing emails
        while st.session_state.is_running and st.session_state.current_index < len(df):
            # Extract the current batch
            print("Hi")
            batch = df.iloc[st.session_state.current_index:st.session_state.current_index + BATCH_SIZE]

            # Process the batch with ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for _, row in batch.iterrows():
                    future = executor.submit(process_row, row.to_dict(), attachment_package, sent_email_log)
                    futures.append(future)

                concurrent.futures.wait(futures)

            # Update index for the next batch
            st.session_state.current_index += BATCH_SIZE
            print(f"Processed batch, waiting 45 seconds...")
            time.sleep(45)  # Wait time between batches

            # Save progress to a JSON file
            temp_progress = {"current_index": st.session_state.current_index}
            with open("progress.json", "w") as temp_file:
                json.dump(temp_progress, temp_file)

        if st.session_state.current_index >= len(df):
            st.session_state.is_running = False
            st.write("All emails have been processed.")
            # Output file for successful emails
            output_df = pd.DataFrame(output_data)
            output_file = "output_emails.csv"
            output_df.to_csv(output_file, index=False)

            st.write("Emails have been sent successfully. Below is the valid email list.")
            st.write(output_df)

            # Download button for the result CSV
            with open(output_file, "rb") as f:
                st.download_button(
                    label="Download Valid Emails CSV",
                    data=f,
                    file_name="valid_emails.csv",
                    mime="text/csv"
                )
else:
    # Show "Resume" and "Stop" buttons once emails have started
    if st.button("Resume Sending Emails"):
        st.session_state.is_running = True
        st.write("Resuming email sending...")

    if st.button("Stop Sending Emails"):
        st.session_state.is_running = False
        st.write("Email sending stopped.")                      


