import smtplib
import datetime
import datetime
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage

# Base path for image files
#basepath = './Report'

# Define these once; use them twice!
strSubject = 'Outlook form Updated successful'

# Create the root message and fill in the from, to, and subject headers
strFrom = 'tianxu@ebay.com'

strTo = ['tianxu@ebay.com']

msgRoot = MIMEMultipart('related')
msgRoot['Subject'] = strSubject
msgRoot['From'] = strFrom
msgRoot['To'] = ', '.join(strTo)
msgRoot.preamble = 'This is a multi-part message in MIME format.'

msgAlternative = MIMEMultipart('alternative')
msgRoot.attach(msgAlternative)

msgText = MIMEText('This is the alternative plain text message.')
msgAlternative.attach(msgText)

#f = open(basepath + '/Report_'+datetime.date.today().strftime('%Y%m%d')+'.html', 'r')
body = "Top Outlook reports have been uploaded in the box, please check the result"
#f.close()
msgText = MIMEText(body, 'html')
msgAlternative.attach(msgText)

smtp = smtplib.SMTP('atom.corp.ebay.com')
smtp.sendmail(strFrom, strTo, msgRoot.as_string())
smtp.quit()

