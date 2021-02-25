import os
import pandas as pd
import json

from db_connection.db_connection import *
from datetime import datetime
from SendEmail.SendEmail import *
from utility_logger.logger_jm import mylogger


def bi_monthly_audit(msg_body):

    # intialize logger
    jm_logger = mylogger(log_file_name='AuditSegment.log')

    # pull data from db
    jm_logger.info('start')


    try:
        df = pd.read_sql("select * from audit_segment",
                         conn_db(db_server='crush_it', db_type='MySQL'))
    except:
        jm_logger.error('Error during querying source db')

    # parse out target information & filter columns & drop duplicates
    def format_col(col):
        try:
            newcol = json.loads(col)
        except:
            newcol = col
        return newcol

    df['new_audit_segment_params'] = df['audit_segment_params'].apply(lambda x: format_col(x))
    df['Email'] = df['new_audit_segment_params'].apply(lambda x: x.get('email', None))
    df['Name'] = df['new_audit_segment_params'].apply(lambda x: x.get('name', None))
    df['Success'] = df['audit_segment_api_res'].apply(lambda x: True if x == 'Success' else False)

    col = ['Email', 'Name', 'audit_coupon', 'audit_timestamp', 'Success']
    df = df[col].drop_duplicates()

    df = df.drop(df[df['audit_coupon'].str.contains('ALLAIN')].index, axis=0)

    # load transformed results to local
    folder = "/Users/jinghanma/Helium10/Adhoc/audit_segment"
    filename = 'audit_segment_' + datetime.today().strftime('%Y%m%d') + '.csv'

    try:
        df.to_csv(os.path.join(folder, filename), index=False)

        # delivery result to audience
        send_email(project='audit_segment',
                   email_subject='Audit Segment Data',
                   msg=msg_body,
                   attachment_path=folder,
                   attachment_name=filename)
        jm_logger.info('job ended with success')

    except:
        jm_logger.error('error during creating csv results')
        # send notification to Data Team
        send_email(project='data_team',
                   email_subject='ERROR in Audit Segment Data',
                   msg='Email Delivery Failed, please refer to log history',
                   attachment_path='/Users/jinghanma/Helium10/auto_jobs/logs',
                   attachment_name='AuditSegment.log')
    return None


if __name__ == "__main__":

    body = """
    Hi team, 

    Please find attached file of coupon data which is pulled from the crushit database.
    Feel free to reach out if you have questions.



    Thanks,
    Jinghan Ma
    Senior Data Analyst
    Helium 10
    500 Technology Drive, Suite 450
    Irvine, CA 92618



    ******
    This is an auto email distribution, 
    please reply STOP to Unsubscribe or reach out to Jinghan Ma to add new contact to recipient list.
    ******
    
    """
    bi_monthly_audit(msg_body=body)