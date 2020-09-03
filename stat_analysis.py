# -*- coding: utf-8 -*-
"""
Created on Sun May 31 16:35:27 2020

@author: Lefteris Mantas
"""
# 1. sum of totals.
# 2. sort countries by each' total, input number of countries, query
# 3. sum each column
# 4. iterate  all years, all files Q1,Q2,Q3,Q4 last sheet last table sum all

from unidecode import unidecode
import requests  # for downloading the files
import sqlite3
from sqlite3 import Error  # for the database
from os import path, mkdir, listdir
#from pandas import DataFrame, read_excel, ExcelFile  # for reading the files' contexts
import xlrd
import sys
from matplotlib import pyplot
import csv

MYDIR = path.dirname(__file__)
DATADIR = MYDIR + "/data/"
if not path.isdir(DATADIR):
   mkdir(DATADIR)
   
RESULTSDIR = MYDIR + "/results/"
if not path.isdir(RESULTSDIR): 
   mkdir(RESULTSDIR)
   
IMAGESDIR = MYDIR + "/images/"
if not path.isdir(IMAGESDIR):
   mkdir(IMAGESDIR)

#################################################################################
def download_excels():
    # create 2 urls the base and the tail in order to connect them with the document id and download the file
    # 2011->2015, 1st->4st
    base_url = 'https://www.statistics.gr/el/statistics?p_p_id=documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_cacheability=cacheLevelPage&p_p_col_id=column-2&p_p_col_count=4&p_p_col_pos=2&_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_javax.faces.resource=document&_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_ln=downloadResources&_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_documentID='
    url_tail = '&_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_locale=el'
    # creating a list with document ids for downloading
    ids_list = [113852, 113856, 113861, 113865,  # 2011
                113871, 113877, 113882, 113886,  # 2012
                113892, 113895, 113901, 113905,  # 2013
                113912, 113917, 113918, 113925,  # 2014
                187618, 187603, 187588, 198755]  # 2015

    print("Downloading...")
    year = 2011  # year initialization
    Q = 1  # quarter initialization
    Q_list = [3, 7, 11, 15, 20]  # points where year changes
    count = 0
    while (count < len(ids_list)):
        full_url = base_url + str(ids_list[count]) + url_tail  # merge the urls
        resp = requests.get(full_url)  # get the data
        down = DATADIR+'data_' + str(year) + "_Q" + str(Q) + ".xls"  # name of data file
        with open(down, 'wb') as out:
            out.write(resp.content)  # write the file to current repository

        if not count in Q_list:  # change the Q
            Q += 1
        else:
            Q = 1; year += 1
        count += 1
    print("Downloading of files was successfully done!")

#################################################################################
def create_connection(db_file):
    """ Create a database connection to a SQLite database """
    conn = None
    try:  # try-except in case of an error in connection
        conn = sqlite3.connect(db_file, timeout=10)
        if conn:
            return conn  # return the connection
    except Error as e:  # in case any problem occurs
        print(e)
        sys.exit(1)

def tables_creation():  # function to create all the needed tables for the data storage
    try:
        conn = create_connection("database.db")  # create the connection to database
        cursor = conn.cursor()

        totals_create = '''CREATE TABLE IF NOT EXISTS totals(
                        year_id INT NOT NULL,
                        year_arrivals INT NOT NULL);'''
        cursor.execute(totals_create)
        print("table totals created")

        countries_create = '''CREATE TABLE IF NOT EXISTS countries( 
                           country_name TEXT NOT NULL,
                           country_arrivals INT NOT NULL);'''
        cursor.execute(countries_create)
        print("table countries created")
        transportation_create = '''CREATE TABLE IF NOT EXISTS transportation(
                              transportation_name TEXT NOT NULL,
                              transportation_arrivals INT NOT NULL);'''
        cursor.execute(transportation_create)
        print("table transportation created")
        quarter_arrivals_create = '''CREATE TABLE IF NOT EXISTS quarter_arrivals(
                              quarter INT NOT NULL,
                              quarter_arrivals INT NOT NULL ); '''

        cursor.execute(quarter_arrivals_create)
        print("table quarter created")
        cursor.close()
        conn.commit()
        conn.close()
        print("All tables were successfully created")
    except Error as e:
        print(e)
        sys.exit(1)

#################################################################################
def hasNumbers(inputString):
    """function that checks is given input has digits.
    This is needed due to the file's format """
    return any(char.isdigit() for char in inputString)

def retrieve_data_from_file_quarters(file):
    '''Question 4: Retrieving the total of each quarter from the sheets of the complete files'''
    book = xlrd.open_workbook(file)
    q = 1 #the 4 quarters of the year
    while (q < 5):
        if q == 1:
            sheet = book.sheet_by_index(2) #get the last sheet of the quarter
        elif q == 2:
            sheet = book.sheet_by_index(5) #get the last sheet of the quarter
        elif q == 3:
            sheet = book.sheet_by_index(8) #get the last sheet of the quarter
        elif q == 4:
            sheet = book.sheet_by_index(11) #get the last sheet of the quarter

        last_row = sheet.row_values(136)
        total = int(last_row[6])
        year = file[-11:-7] #get the 201x year
        quarter = year + "_Q" + str(q) #201x_Qx year and quarter
        record = [] #create a record to insert into database
        record.append(quarter)
        record.append(total)
        insert_data(record, '4th')
        q += 1

def retrieve_data_from_file(file): #function that processes the file and retrieves the needed data for the database
    book = xlrd.open_workbook(file)
    last_sheet = book.sheet_by_index(11)  # read the last sheet

    '''Question 1: Retrieving the total of arrivals'''
    last_row = last_sheet.row_values(136)
    total = int(last_row[6])
    year = int(file[-11:-7]) #the 201x year
    record=[]
    record.append(year)
    record.append(total)
    insert_data(record, '1st')

    '''Question 2: Retrieving the countries and totals of arrivals'''
    for i in range(76, 135):  # Austria->Okeanian Countries
        row = last_sheet.row_values(i)
        if hasNumbers(row[0]):  # exclude rows without data
            if '2011' in file:
               insert_data(row, '2nd')  # insert first time
            else:
               insert_data(row, '2nd_update')  # update for the rest of the times

    '''Question 3: Retrieving the transportations and totals of arrivals'''
    transport_names_row = last_sheet.row_values(74)
    total_arrivals_row = last_sheet.row_values(136)

    transportation_names = transport_names_row[2:-2] #get only the necessary columns: plane, train, ship
    transportation_totals = total_arrivals_row[2:-2] #get their arrival totals

    for name, arrivals in zip(transportation_names, transportation_totals):
        record = []
        record.append(unidecode(name))
        record.append(int(arrivals))
        if '2011' in file:
            insert_data(record, '3rd')
        else:
            insert_data(record, '3rd update')  # update for the rest of the times

    retrieve_data_from_file_quarters(file)
    print("data from file {} successfully inserted into database".format(file))

def insert_data(record, choice):  # function for inserting data into tables
    conn = create_connection("database.db")  # create the connection to database
    cursor = conn.cursor()

    # check all possible insertions into tables
    if choice == '1st':
        entry = (record[0], record[1])

        insert_totals = '''INSERT INTO totals(year_id,year_arrivals) VALUES(?,?)'''
        cursor.execute(insert_totals, entry)

    elif choice == '2nd':
        entry = ("'" + unidecode(record[1]) + "'", record[6])
        insert_countries = '''INSERT INTO countries(country_name,country_arrivals)
                           VALUES (?,?);'''
        cursor.execute(insert_countries, entry)

    elif choice == '2nd_update':
        entry = ("'" + unidecode(record[1]) + "'", record[6])
        select_country = '''SELECT country_arrivals FROM countries 
                         WHERE country_name == "{}" ;'''.format(entry[0])
        cursor.execute(select_country)
        arrivals = cursor.fetchall()
        sum_arrivals = 0
        
        for a in arrivals:
           sum_arrivals += int(a[0]) #sum all the previous arrivals

        new_arrival = sum_arrivals + int(record[6]) #the updated number of arrivals is the previous total + the new value

        update_countries = '''UPDATE countries SET country_arrivals = {} 
                           WHERE country_name == "{}" '''.format(new_arrival, "'" +unidecode(record[1]) + "'")
        cursor.execute(update_countries)

    elif choice == '3rd':
        entry = (record[0], record[1])
        insert_transportation = '''
        INSERT INTO transportation(transportation_name,transportation_arrivals)
        VALUES (?,?);'''
        cursor.execute(insert_transportation, entry)

    elif choice == '3rd update':
        
        #entry = (record[0], record[1])
        select_transportation = '''SELECT transportation_arrivals FROM transportation
                               WHERE transportation_name == "{}" ;'''.format(record[0])
        cursor.execute(select_transportation)
        arrivals = cursor.fetchall()
        sum_arrivals = 0
        for a in arrivals:
            sum_arrivals += int(a[0]) #sum all the previous arrivals

        new_arrival = sum_arrivals + int(record[1]) #the updated number of arrivals is the previous total + the new value

        update_transportation = '''UPDATE transportation SET transportation_arrivals = {} 
                           WHERE transportation_name == "{}" '''.format(new_arrival, record[0])
        cursor.execute(update_transportation)

    elif choice == '4th':
        entry = (record[0], record[1])
        insert_quarter = '''INSERT INTO quarter_arrivals(quarter,quarter_arrivals) VALUES (?,?);'''
        cursor.execute(insert_quarter, entry)

    conn.commit()
    cursor.close()
    conn.close()

#################################################################################
def queries():
    conn = create_connection("database.db")  # create the connection to database
    cursor = conn.cursor()

    """Question 1"""
    select = '''SELECT * FROM totals;'''
    cursor.execute(select)
    totals = cursor.fetchall()  # retrieve the answer of the query

    years = [row[0] for row in totals] #creating list of years for plot
    arrivals = [row[1] for row in totals] #creating list of arrivals for plot
    plot_results(years, arrivals,'Total arrivals between 2011-2015 ','Arrivals','Years')
    write_results_to_csv(years, arrivals, "results_1",1)

    """Question 2"""
    select = '''SELECT * FROM countries;'''
    cursor.execute(select)
    countries = cursor.fetchall()  # retrieve the answer of the query

    countries_names = [row[0] for row in countries]  # creating list of countries for plot
    arrivals = [row[1] for row in countries]  # creating list of arrivals for plot
    
    plot2(countries_names, arrivals, "Total arrivals between 2011-2015 per Country", "Countries", "Arrivals")
    write_results_to_csv(countries_names, arrivals, "results_2",2)

    """Question 3"""
    select = '''SELECT * FROM transportation;'''
    cursor.execute(select)
    transportation = cursor.fetchall()  # retrieve the answer of the query

    transports = [row[0] for row in transportation]  # creating list of transports for plot
    arrivals = [row[1] for row in transportation]  # creating list of arrivals for plot

    plot2(transports, arrivals, "Total arrivals between 2011-2015 per Transportation", "Transportation",
                   "Arrivals")
    write_results_to_csv(transports, arrivals, "results_3",3)

    """Question 4"""
    select = '''SELECT * FROM quarter_arrivals;'''
    cursor.execute(select)
    quarters_all = cursor.fetchall()  # retrieve the answer of the query

    quarters = [row[0] for row in quarters_all]  # creating list of quarters for plot
    arrivals = [row[1] for row in quarters_all]  # creating list of arrivals for plot

    plot_results(quarters, arrivals, 'Total arrivals between 2011-2015 per Quarter', 'Arrivals', 'Quarters')
    write_results_to_csv(quarters, arrivals, "results_4",4)
    cursor.close()



#################################################################################
def plot_results(time, object, title, xlabel, ylabel):
    """ function for plotting the results """
    # for questions 1 and 4
    pyplot.figure(figsize=(20,10))
    pyplot.plot(time, object, color='blue')
    pyplot.title(title)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)

    pyplot.legend('Arrivals', loc='upper right')
    if "Quarter" in title:
       pyplot.savefig(IMAGESDIR+"quarters.png")
    else:
       pyplot.savefig(IMAGESDIR+"totals.png")
    
    

def plot2(object, arrivals, title, xlabel, ylabel):
    """ function for creating a histogram of the results """
    # for questions 2 and 3
    if "Country" in title:
       fig, axs=pyplot.subplots(1,1,figsize=(30,10))
    else:
       fig, axs= pyplot.subplots(1,1,figsize=(20,10))
       
    pyplot.plot(object, arrivals, color='blue')
    pyplot.title(title)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.grid(axis='y', alpha=0.75)
    if "Country" in title:
       pyplot.savefig(IMAGESDIR+"countries.png")
    else:
       pyplot.savefig(IMAGESDIR+"transportation.png")



def write_results_to_csv(columns1, columns2, filename, question):
    '''
    rows: what goes to 1st column
    columns: what goes to 2snd column
    MYDIR is public
    filename is results_1, results_2, results_3, results_4
    question is 1,2,3,4
    '''

    with open(RESULTSDIR + filename + ".csv", 'w',encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',')
        if question == 1:
            writer.writerow(["Year", "Arrivals"])  # header

        elif question == 2:
            writer.writerow(["Country", "Arrivals"])  # header

        elif question == 3:
            writer.writerow(["Transportation", "Arrivals"])  # header

        elif question == 4:
            writer.writerow(["Quarter", "Arrivals"])  # header

        for col1, col2 in zip(columns1, columns2):
            writer.writerow([col1, col2])
    file.close()

#################################################################################
def main():
    #download_excels() #function to download all the necessary files from website

    files = listdir('{}'.format(DATADIR))  # get path of directory
    tables_creation()

    for file in files:  # iterate through files
        path = DATADIR + file
        '''questions 1 and 2 and 3'''
        if 'Q4' in path:
            retrieve_data_from_file(path)  # read the file
    queries()

if __name__ == '__main__':
    main()
