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
name_sender="Rounak Raman (NSUT)"

# Function to get relevant field
def getRelevantField(company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Changed model name to correct version
        response = model.generate_content(
            f"What is the relevant field in which the {company} is working? Mention only the prominent field in short",
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=12,
                temperature=1.5,
            )
        )
        return response.text
    except Exception as e:
        print(f"Error getting relevant field: {e}")
        return "technology"  # fallback value

# Function to get subject
def getSubject(name, company):
    try:
        model = genai.GenerativeModel("gemini-1.0-pro")  # Changed model name
        response = model.generate_content(
            f"What should be the subject of the email to {name} for a referral at {company}?",
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=12,
                temperature=1.5,
            )
        )
        return response.text
    except Exception as e:
        print(f"Error getting subject: {e}")
        return f"Referral Request for {company}"  # fallback subject

def send_email(receiver_email, name, relevant_field, attachment_package, company):
    try:
        print("Hello")
        msg = EmailMessage()
        print("Hi")
        msg['Subject'] = getSubject(name, company)
        msg['From'] = formataddr((f"{name_sender}", email))
        msg['To'] = receiver_email

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
        print("Hola")
        msg.set_content(plain_text)
        msg.add_alternative(html_content, subtype="html")
        msg.add_attachment(attachment_package.get_payload(decode=True), maintype='application', subtype='octet-stream', filename=attachment_package.get_filename())

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as connection:
            connection.login(user=email, password=password_1)
            connection.send_message(msg)
        print(f"Email successfully sent to {receiver_email}")
    except Exception as e:
        print(f"Error sending email: {e}")

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
    except Exception as e:
        print(f"Error processing row: {e}")

if __name__ == "__main__":
    try:
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
            print(f"Batch completed, waiting 120 seconds...")
            time.sleep(120)

    except Exception as e:
        print(f"Main process error: {e}")