import argparse
import boto3
import datetime
import functools 
import pandas as pd
import statistics 
import sys

cost_explorer = boto3.client('ce', 'eu-west-1')

period = 30
prod_account = '743677148328'
service_id = 1

variance = {}
results = [] 
results_delta = [] 
serviceCostDict = {}
serviceCostDeltaDict = {}
standardDeviation = {}
intervalDict = {'start': 0, 'end':0}

with open('temp.txt','w') as t:

    def calculate_cost_interval(intervalDict, length_days):
        now = datetime.datetime.utcnow()
        intervalDict['start'] = (now - datetime.timedelta(days=length_days)).strftime('%Y-%m-%d')
        intervalDict['end'] = now.strftime('%Y-%m-%d') 


    def unique(original_list):  
        return (list(set(original_list)))


    def get_cost_and_usage(start, end):
        local_results = [] 
        token = None
        while True:
            if token:
                kwargs = {'NextPageToken': token}
            else:
                kwargs = {}
            data = cost_explorer.get_cost_and_usage(TimePeriod={'Start': start, 'End':  end}, Granularity='DAILY',
                   Filter={'Dimensions': {'Key':'LINKED_ACCOUNT', 'Values': [prod_account]}}, Metrics=['UnblendedCost'], 
                   GroupBy=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}, {'Type': 'DIMENSION', 'Key': 'SERVICE'}], **kwargs)
            local_results += data['ResultsByTime']
            token = data.get('NextPageToken')
            if not token:
                break
        return local_results


    def calculate_total_cost(cost_results, unique_list):
        localDict = {}
        for service in unique_list:
            cost_list = [float(group['Metrics']['UnblendedCost']['Amount']) for result_by_time in cost_results for group in result_by_time['Groups'] 
                            if group['Keys'][service_id] == service]
            localDict[service] = sum(cost_list)
        return localDict


    def calculate_std_deviation(cost_results, unique_list):
    
        vari = {}
        print("start: calculate_std_deviation" + '\n', file=t)
        for service in unique_list:

            cost_list = [float(group['Metrics']['UnblendedCost']['Amount']) for result_by_time in cost_results for group in result_by_time['Groups'] 
                            if group['Keys'][service_id] == service]
            if len(cost_list) > 1: vari[service] = statistics.stdev(cost_list, xbar=None) 
            print("Service: " + service + ' ' + "Std Dev: " + statistics.stdev(cost_list, xbar=None))
        print("end: calculate_std_deviation" + '\n', file=t)
        return vari


    def create_unique_service_list(local_results):
        print("start: service list" + '\n', file=t)
        local_service_list = [group['Keys'][service_id] for result_by_time in local_results for group in result_by_time['Groups']]
        local_unique_service_list = unique(local_service_list)
        print("service: " + str(local_unique_service_list) + '\n', file=t)
        print("end: service list" + '\n', file=t)
        return local_unique_service_list


    def check_lists_are_identical(list_full, list_delta):
        return (list(set(list_full) - set(list_delta))) 

    def use_only_common_elements(list_full, list_delta):    
        return list(set(list_full) & set(list_delta))


    calculate_cost_interval(intervalDict, 30)
    results = get_cost_and_usage(intervalDict['start'], intervalDict['end'])

    calculate_cost_interval(intervalDict, 1)
    results_daily = get_cost_and_usage(intervalDict['start'], intervalDict['end'])

    unique_service_list = create_unique_service_list(results)
    unique_service_list_delta = create_unique_service_list(results_daily)

    common_unique_service_list = use_only_common_elements(unique_service_list, unique_service_list_delta)

    variance = calculate_std_deviation(results, common_unique_service_list)

    serviceCostDict = calculate_total_cost(results, common_unique_service_list)
    serviceCostTodayDict = calculate_total_cost(results_daily, common_unique_service_list)

    for key in serviceCostDict:
        delta = (serviceCostDict[key]/30) - serviceCostTodayDict[key]
        dtemp = abs(variance[key]) - abs(delta) 

        print('Service: ' + key + 'Avg Daily Cost: ' + (serviceCostDict[key]/30) + ' Cost Today: ' + serviceCostTodayDict[key] + '  Diff/Delta: ' + str(delta))
        print('Service: ' + key + '  ' + ' Std Deviation - Delta: ' + str(dtemp))
    print('\n','\n')

    [(print(service_name + " " + str(service_cost) + " " + "mean: " + str(service_cost/30))) for service_name, service_cost in serviceCostDict.items()]

    print('\n', "Services that do not appear in both lists are: " + '\n' + str(check_lists_are_identical(unique_service_list, unique_service_list_delta)) + '\n')