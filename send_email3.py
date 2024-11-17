from email import encoders
from email.mime.base import MIMEBase
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import pandas as pd
import concurrent.futures
import time
from dotenv import load_dotenv
import os
import google.generativeai as genai  

load_dotenv()

# Sender's credentials

email = "ramanrounak.work@gmail.com"
password_1 = os.getenv("PASSWORD")
API_KEY = os.getenv("API")
genai.configure(api_key=API_KEY)


# Function to send an individual email

def getRelevantField(company):
    model = genai.GenerativeModel("gemini-1.5-flash-8b")
    response = model.generate_content(f"What is the relevant field in which the {company} is working? Mention only the prominent field in short",generation_config=genai.types.GenerationConfig(
        
        max_output_tokens=12,
        temperature=1.5,
    ))
    return response.text

def getSubject(name,company):
    model = genai.GenerativeModel("gemini-1.5-flash-8b ")
    response = model.generate_content(f"What should be the subject of the email to {name} for a referral at {company}?",generation_config=genai.types.GenerationConfig(
        
        max_output_tokens=12,
        temperature=1.5,
    ))
    return response.text


def send_email(receiver_email, name, relevant_field, attachment_package, company):
    msg = EmailMessage()
    msg['Subject'] = getSubject(name,company)
    msg['From'] = formataddr(("Rounak Raman", f"{email}"))
    msg['To'] = receiver_email
    relevant_field = getRelevantField(company)


    # Add a plain text version of the email
    plain_text = f"""
    Hi {name},

    I hope you're doing well! I'm Rounak Raman, currently pursuing a B.Tech in Information Technology at NSUT, with extensive experience in data analysis and machine learning. My recent work includes developing predictive models at DRDO-INMAS and building data-driven dashboards for pollution reduction strategies at Nation With Namo.

    I noticed your company's focus on {relevant_field}, and I believe my skills in data science and experience with end-to-end pipelines could be a great fit. I would love to explore any open positions where I could contribute.

    If you're open to it, I would be grateful if you could refer me for a suitable role in your team.

    Thank you for considering my request!

    Best regards,
    Rounak Raman
    """

    # Add a more engaging HTML version of the email
    html_content = f'''
    <html>
    <body>
        <p>Hi {name},</p>
        <p>I hope you're doing well! I'm <strong>Rounak Raman</strong>, currently pursuing a B.Tech in Information Technology at NSUT, with extensive experience in data analysis and machine learning. My recent work includes developing predictive models at <strong>DRDO-INMAS</strong> and building data-driven dashboards for pollution reduction strategies at <strong>Nation With Namo</strong>.</p>
        <p>I noticed your company's focus on <strong>{relevant_field}</strong>, and I believe my skills in data science and experience with end-to-end pipelines could be a great fit. I would love to explore any open positions where I could contribute.</p>
        <p>If you're open to it, I would be grateful if you could refer me for a suitable role in your team.</p>
        <p>Thank you for considering my request!</p>
        <p>Best regards,</p>
        <p><strong>Rounak Raman</strong></p>
    </body>
    </html>
    '''

    # Attach the text and HTML versions to the email
    msg.set_content(plain_text)  # Plain text fallback
    msg.add_alternative(html_content, subtype="html")  # HTML version

    # Attach the resume
    msg.attach(attachment_package)

    # Sending the email with proper error handling
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as connection:
            connection.login(user=email, password=password_1)
            connection.send_message(msg)
        print(f"Email successfully sent to {receiver_email}")
    except Exception as e:
        print(f"Error sending email to {receiver_email}: {e}")

# Function to handle each row of data in parallel
def process_row(row, attachment_package):
    n1 = row["Name"]  # Recipient's Name
    n2 = row["Company name"]  # Company Name
    n3 = row["emails"]  # Recipient's Email
    print("3")

    try:
        print("Hello")
        # Send the email with the updated parameters
        send_email(
            
            receiver_email=n3,
            name=n1,
            relevant_field=getRelevantField(n2),  # Use the company name to get the relevant field
            attachment_package=attachment_package,
            company=n2  # Company name for reference
        )
        print(f"Email successfully sent to {n3}")
    except Exception as e:
        print(f"Error sending email to {n3}: {e}")
  

if __name__ == "__main__":
    # Reading the CSV file in chunks (optimization for large datasets)
    df = pd.read_csv("data.csv")

    # Reading the resume file once and reusing the attachment
    filename = "Rounak_Raman_Resume.pdf"
    with open(filename, 'rb') as attachment:
        # Create a MIME package for the attachment
        attachment_package = MIMEBase('application', 'octet-stream')
        attachment_package.set_payload(attachment.read())
        encoders.encode_base64(attachment_package)
        attachment_package.add_header('Content-Disposition', f"attachment; filename={filename}")

    # Batch size to avoid sending too many emails at once (helps avoid server rate-limiting)
    BATCH_SIZE = 1  # Adjust this value based on needs
    x = 0

    while x < len(df):
        # Process emails in batches of size BATCH_SIZE
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Prepare a batch
            print("Hi")
            batch = df.iloc[x:x + BATCH_SIZE].to_dict('records')
            print("2")
            # Map each row of the batch to the process_row function and send emails in parallel
            executor.map(lambda row: process_row(row, attachment_package), batch)

        # Move to the next batch
        x += BATCH_SIZE

        ##Pause between batches to avoid overwhelming the email server
        print(f"Batch {x//BATCH_SIZE} processed, pausing for 120 seconds...")
        time.sleep(120)

