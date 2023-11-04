from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import gspread
import yfinance as yf
from gspread_dataframe import set_with_dataframe
from airflow.operators.email_operator import EmailOperator
from forex_python.converter import CurrencyRates
import matplotlib.pyplot as plt
import pytz

Sheet_Name = 'My Investment'
Market_sheet = 'Market Value'
Investment_sheet = 'Investment'

gc = gspread.service_account(filename='creds/myinvestment-403711-f60632e19249.json')
sh = gc.open(Sheet_Name)

def Extract(**kw):
    Market_value_Worksheet = sh.worksheet(Market_sheet)
    Market_df = pd.DataFrame(Market_value_Worksheet.get_all_records())
    Investment_Worksheet = sh.worksheet(Investment_sheet)
    Investment_df = pd.DataFrame(Investment_Worksheet.get_all_records())

    return Market_df, Investment_df

def Transformation(**kw):
    ti = kw['ti']
    Market_df = ti.xcom_pull(task_ids='Extract', key=None)[0]
    Investment_df = ti.xcom_pull(task_ids='Extract', key=None)[1]

    Market_df = pd.DataFrame(Market_df)
    Investment_df = pd.DataFrame(Investment_df)

    for i, row in Market_df.iterrows():
        if row['Fund_ID'] == 'Exchange':
            Dollar = row['MKT Price']

    Investment_df['Total Qty'] = 0.0
    for fund_id, group in Investment_df.groupby('Fund_ID'):
        for i, row in group.iterrows():
            if row['Category'] == 'Invested':
                if i == group.index[0]:
                   Investment_df.loc[i, 'Total Qty'] = row['Qty']
                else:
                    Investment_df.loc[i, 'Total Qty'] = round(Investment_df.loc[i-1, 'Total Qty'], 3) + row['Qty']
            else:
                Investment_df.loc[i, 'Total Qty'] = round(Investment_df.loc[i-1, 'Total Qty'], 3) - row['Qty']


    Investment_df['Net Amount'] = 0
    Investment_df['Status'] = 1
    for fund_id, group in Investment_df.groupby('Fund_ID'):
        for i, row in group.iterrows():
            if row['Category'] == 'Invested':
                if i == group.index[0]:
                    Investment_df.loc[i,'Net Amount'] = row['Amount']
                else:
                    Investment_df.loc[i,'Net Amount'] = Investment_df.loc[i-1,'Net Amount'] + row['Amount']
                    Investment_df.loc[i-1,'Status'] = 0

            else:
                if row['Category'] == 'Redeemed' or row['Total Qty'] > 0:
                    Investment_df.loc[i,'Net Amount'] = (Investment_df.loc[i-1,'Net Amount']/Investment_df.loc[i-1,'Total Qty'])*row['Total Qty']
                    Investment_df.loc[i-1,'Status'] = 0
                else:
                    Investment_df.loc[i,'Net Amount'] = 0
                    Investment_df.loc[i,'Status'] = 0

    Active_Investment = Investment_df.loc[Investment_df['Status']==1]

    Merge_df = pd.merge(Market_df, Active_Investment, on=['Fund_ID', 'Fund Name'], how='inner')
    Merge_df['Current Value'] = Merge_df['MKT Price'] * Merge_df['Total Qty']
    Merge_df['Profit'] = Merge_df['Current Value'] - Merge_df['Net Amount']
    Merge_df['Profit %'] = ((Merge_df['Current Value'] / Merge_df['Net Amount']) * 100) - 100

    Active_Investment.to_csv('./active_investment.csv', index=False)
    Investment_df.to_csv('./investment.csv', index=False)
    Market_df.to_csv('./market.csv', index=False)
    Merge_df.to_csv('./merged.csv', index=False)

    return Merge_df, Dollar

def Load(**kw):
    ti = kw['ti']
    merge_df = ti.xcom_pull(task_ids='Transformation', key=None)[0]
    merge_df = pd.DataFrame(merge_df)

    ist = pytz.timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist)

    current_date = current_time_ist.strftime('%Y-%m-%d')

    for index, row in merge_df.iterrows():
        Fund_ID = row['Fund_ID']
        US_IND = row['US/IND']
        Date = current_date
        InvestedAmount = row['Net Amount']
        TotalValue = row['Current Value']
        Exchange = row['Exchange Rate']
        Tax = row['Tax']
        Profit = row['Profit']
        Profit_pec = row['Profit %']

        append_list = [Fund_ID, US_IND, Date, InvestedAmount, TotalValue,  Exchange, Tax, Profit, Profit_pec]

        Investment_Growth = sh.worksheet('Finance DWH')
        Investment_Growth.append_row(append_list)


def Report(**kw):
    ti = kw['ti']
    FinanceDWH = sh.worksheet('Finance DWH')
    FinanceDWH = pd.DataFrame(FinanceDWH.get_all_records())
    Dollar = ti.xcom_pull(task_ids='Transformation', key=None)[1]

    ist = pytz.timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist)

    Today = current_time_ist.strftime('%Y-%m-%d')

    Daily_data = FinanceDWH.loc[FinanceDWH['Date'] == Today]
    Net_Invested = 0
    US_Investment = 0
    US_Net_Investment = 0
    Net_Total_Value = 0
    US_Total_Value = 0
    US_Net_Total_Value = 0
    IND_Net_Total_Value = 0
    IND_Net_Investment = 0

    for i, row in Daily_data.iterrows():
        if row['US_IND'] == 'IND':
            Net_Invested = row['Invested Amount'] + Net_Invested
            IND_Net_Investment = row['Invested Amount'] + IND_Net_Investment
            Net_Total_Value = row['Total Value'] + Net_Total_Value
            IND_Net_Total_Value = row['Total Value'] + IND_Net_Total_Value

        else:
            US_Investment = row['Invested Amount']*row['Exchange']
            Net_Invested = US_Investment + Net_Invested
            US_Net_Investment = US_Investment + US_Net_Investment
            US_Total_Value = row['Total Value']*Dollar
            Net_Total_Value = US_Total_Value + Net_Total_Value
            US_Net_Total_Value = US_Total_Value + US_Net_Total_Value



    Net_Profit = Net_Total_Value-Net_Invested
    US_Profit = US_Net_Total_Value-US_Net_Investment
    IND_Profit = IND_Net_Total_Value - IND_Net_Investment

    Ind_append_list = ['IND',Today,IND_Net_Investment, IND_Net_Total_Value, IND_Profit]
    us_append_list = ['US',Today,US_Net_Investment, US_Net_Total_Value, US_Profit]

    IND_US_Growth = sh.worksheet('IND&US Growth')
    IND_US_Growth.append_row(Ind_append_list)
    IND_US_Growth.append_row(us_append_list)

    def US_IND_Investment_Plot():
        labels = ['US', 'IND']
        value = [US_Net_Investment, IND_Net_Investment]

        plt.figure(figsize=(6, 6))
        # Custom colors for the pie chart
        colors = ['#ff9999', '#66b3ff']
        # Create a pie chart with custom colors
        fig, ax = plt.subplots()
        ax.pie(value, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors)
        # Equal aspect ratio ensures that the pie is drawn as a circle
        ax.axis('equal')
        centre_circle = plt.Circle((0, 0), 0.50, fc='white')
        fig = plt.gcf()
        # Adding Circle in Pie chart
        fig.gca().add_artist(centre_circle)
        # Add a title
        plt.title('Investment Distribution')
        plt.savefig('./donet.png')
    
    US_IND_Investment_Plot()

    return Net_Invested, Net_Total_Value, Net_Profit, US_Net_Investment, US_Net_Total_Value, US_Profit, IND_Net_Investment, IND_Net_Total_Value, IND_Profit

def send_email(**kw):
    ist = pytz.timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist)
    current_date = current_time_ist.strftime('%Y-%m-%d')

    ti = kw['ti']

    Net_Invested = ti.xcom_pull(task_ids='Report', key=None)[0]
    Net_Total_Value = ti.xcom_pull(task_ids='Report', key=None)[1]
    Net_Profit = ti.xcom_pull(task_ids='Report', key=None)[2]
    US_Net_Investment = ti.xcom_pull(task_ids='Report', key=None)[3]
    US_Net_Total_Value = ti.xcom_pull(task_ids='Report', key=None)[4]
    US_Profit = ti.xcom_pull(task_ids='Report', key=None)[5]
    IND_Net_Investment = ti.xcom_pull(task_ids='Report', key=None)[6]
    IND_Net_Total_Value = ti.xcom_pull(task_ids='Report', key=None)[7]
    IND_Profit = ti.xcom_pull(task_ids='Report', key=None)[8]


    Investment_Growth = sh.worksheet('Finance DWH')
    data = Investment_Growth.get_all_values()
    dates = [row[2] for row in data]

    template_file = "email_template.html"
    with open(template_file) as file:
        email_content = file.read()

    if current_date in dates:
        # Populate the HTML content with appropriate values
        email_content = email_content.format(
            Date = current_date,
            invested_value= round(Net_Invested),
            total_value=round(Net_Total_Value),
            net_profit=round(Net_Profit),
            us_net_investment=round(US_Net_Investment),
            us_net_total_value=round(US_Net_Total_Value),
            us_profit=round(US_Profit),
            ind_net_investment=round(IND_Net_Investment),
            ind_net_total_value=round(IND_Net_Total_Value),
            ind_profit=round(IND_Profit)
        )

        email_task = EmailOperator(
            task_id='send_email',
            to='boobathya1412@gmail.com',
            subject="Today's Report",
            html_content=email_content
        )
        email_task.execute(context=kw)
    else:
        print('Airflow job failed')
        email_content = """
        Hello,
        Your Airflow job failed as today's data is not available.
        
        Regards,
        Airflow
        """

default_args = {
    "owner": "Boobathy A",
    'start_date': datetime(2023, 10, 31)
}

dag = DAG('My_Investment',
          default_args=default_args, 
         schedule_interval=None # You can adjust this based on your requirements
          )

Extract_Task = PythonOperator(
    task_id='Extract',
    python_callable=Extract,
    provide_context=True,
    dag=dag
)

Transformation_Task = PythonOperator(
    task_id='Transformation',
    python_callable=Transformation,
    provide_context=True,
    dag=dag
)

Load_Task = PythonOperator(
    task_id='Load',
    python_callable=Load,
    provide_context=True,
    dag=dag
)

Report_Task = PythonOperator(
    task_id = 'Report',
    python_callable = Report,
    dag=dag
)

Mail_Task = PythonOperator(
    task_id = 'mail',
    python_callable = send_email,
    dag=dag
)

Extract_Task >> Transformation_Task >> Load_Task >> Report_Task >>Mail_Task