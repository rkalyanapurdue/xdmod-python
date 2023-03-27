from xdmod.datawarehouse import DataWareHouse
import csv

dw = DataWareHouse(xdmodhost='https://xdmod.anvil.rcac.purdue.edu',sslverify=False)

dw.__enter__()

def get_jobids():

    batchSize = 500

    try:
      count = batchSize
      start = 0
      jobids = []
      while True:
          (totalCount,result) = dw.getjobs(start_date='2022-12-01',end_date='2022-12-31',count=count,start=start)
          jobids = jobids + result
          # find new start and count
          if (start + count) > totalCount:
              break
          else:
              start = start + count
          print("fetch %d records from %d" % (count,start))
      print("found %d jobs" % len(jobids))
      with open('jobids-dec.txt','w') as jobidFile:
          for jobID in jobids:
              jobidFile.write('%s\n' % jobID)
    except:
      raise

def get_jobaccounting():

    with open('job_accounting_nov.csv','w') as csvfile:
        fieldnames = ['Account','Job Id','Shared','Cores','Gpus','Nodes','Cpu Time','Node Time','Requested Nodes','Requested Wall Time','Queue','Wait Time','Wall Time','Eligible Time','End Time','Start Time','Submit Time','User','Exit Status','Hosts','Job Name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,extrasaction='ignore')
        writer.writeheader()
        with open('jobids-nov.txt','r') as jobidfile:
            for line in jobidfile:
                jobid = str(line).rstrip()
                res = dw.jobaccountingdata(jobid)
                #map(res.pop,['Total Cores Available','Local Job Id','Name','Organization','Resource','Hierarchy Bottom Level','PI','PI Institution','Timezone','User Institution','Username','Application','Executable','Working directory'])

                res['Job Id'] = jobid
                res['Job Name'] = res['Name']

                writer.writerow(res)

def get_jobmetrics():
    fieldnames = ['Job Id','Device','Device Name','Metric','Unit','Average','Count','Standard Deviation','Median','Skew','Minimum','Maximum','Coefficient of Variance','Kurtosis']
    metric_field_mapping = {'Metric':'name','Unit':'unit','Average':'avg','Count':'cnt','Standard Deviation':'std','Median':'med','Skew':'skw','Minimum':'min','Maximum':'max','Coefficient of Variance':'cov','Kurtosis':'krt'}

    with open('job_metrics_5.csv','w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,extrasaction='ignore')
        writer.writeheader()
        with open('jobids-5.txt','r') as jobidfile:
            for line in jobidfile:
                jobid = str(line).rstrip()
                res = dw.jobperformancedata(jobid)

                # loop through devices
                for device in res:
                    if device['name'] != 'cores':
                        if 'children' in device:
                            for devices in device['children']:
                                if 'children' in devices:
                                    for metrics in devices['children']:
                                        record = dict()
                                        record['Job Id'] = jobid
                                        record['Device'] = device['name']
                                        record['Device Name'] = devices['name']
                                        for field,mapped_field in metric_field_mapping.items():
                                            if mapped_field in metrics:
                                                record[field] = metrics[mapped_field]
                                            else:
                                                record[field] = None
                                        writer.writerow(record)
                                #else assume these are metrics
                                else:
                                    record = dict()
                                    record['Job Id'] = jobid
                                    record['Device'] = device['name']
                                    record['Device Name'] = None
                                    for field,mapped_field in metric_field_mapping.items():
                                        if mapped_field in devices:
                                            record[field] = devices[mapped_field]
                                        else:
                                            record[field] = None
                                    writer.writerow(record)

def get_jobhosts():
    with open('job_accounting_nov.csv','r') as srcfile:
        reader = csv.DictReader(srcfile)
        fieldnames = reader.fieldnames
        fieldnames.append('Hosts')
        with open('job_accounting_nov_hosts.csv','w') as resfile:
            writer = csv.DictWriter(resfile,fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                hosts = dw.jobhostdata(row['Job Id'])

                row['Hosts'] = ','.join(hosts)

                writer.writerow(row)

def get_jobtimeseries():
    with open('job_timeseries_aug.csv','w') as timesfile:
        fieldnames = ['Job Id','Host','Event','Value','Timestamp','Units']
        writer = csv.DictWriter(timesfile,fieldnames=fieldnames)
        writer.writeheader()

        with open('jobids-aug.txt','r') as jobids:
            for jobid in jobids:
                res = dw.jobtimeseries(jobid)

                for key in res.keys():
                    event = key

                    event_data = res[event]

                    units = event_data['units']

                    for host in event_data['data'].keys():
                        host_data = event_data['data'][host]

                        for data in host_data:
                            unix_timestamp = data['x']
                            value = data['y']

                            row = dict()

                            row['Job Id'] = jobid
                            row['Host'] = host


def get_job_metrics():
    with open('jobids-nov.txt','r') as jobids:
        with open('job_timeseries_nov.json','w') as timefile:
            for jobid in jobids:
                res = dw.jobtimeseries(jobid)
                timefile.write('%s\n' % res)

get_jobids()
#get_jobaccounting()
#get_jobhosts()
#get_job_metrics()





