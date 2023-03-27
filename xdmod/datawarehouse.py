from datetime import datetime
import io
import tempfile
import json
import os
import csv
from urllib.parse import urlencode
import re
import html

import numpy
import pycurl
import pandas as pd


class DataWareHouse:
    """ Access the XDMoD datawarehouse via XDMoD's network API """

    def __init__(self, xdmodhost, apikey=None, sslverify=True):
        self.xdmodhost = xdmodhost
        self.apikey = apikey
        self.logged_in = None
        self.crl = None
        self.cookiefile = None
        self.descriptor = None
        self.sslverify = sslverify
        self.headers = []

        if not self.apikey:
            try:
                self.apikey = {
                    'username': os.environ['XDMOD_USER'],
                    'password': os.environ['XDMOD_PASS']
                }
            except KeyError:
                pass

    def __enter__(self):
        self.crl = pycurl.Curl()

        if not self.sslverify:
            self.crl.setopt(pycurl.SSL_VERIFYPEER, 0)
            self.crl.setopt(pycurl.SSL_VERIFYHOST, 0)

        if self.apikey:
            _, self.cookiefile = tempfile.mkstemp()
            self.crl.setopt(pycurl.COOKIEJAR, self.cookiefile)
            self.crl.setopt(pycurl.COOKIEFILE, self.cookiefile)

            self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/auth/login')
            pf = urlencode(self.apikey)
            b_obj = io.BytesIO()
            self.crl.setopt(pycurl.WRITEDATA, b_obj)
            self.crl.setopt(pycurl.POSTFIELDS, pf)
            self.crl.perform()

            response = json.loads(b_obj.getvalue().decode('utf8'))
            if response['success'] is True:
                token = response['results']['token']
                self.headers = ['Token: ' + token]
                self.crl.setopt(pycurl.HTTPHEADER, self.headers)
                self.logged_in = response['results']['name']
            else:
                raise RuntimeError('Access Denied')

        return self

    def __exit__(self, tpe, value, tb):
        if self.cookiefile:
            os.unlink(self.cookiefile)
        if self.crl:
            self.crl.close()
        self.logged_in = None

    def whoami(self):
        if self.logged_in:
            return self.logged_in
        return "Not logged in"

    def realms(self):
        info = self.get_descriptor()
        return [*info['realms']]

    def metrics(self, realm):
        info = self.get_descriptor()
        output = []
        for metric, minfo in info['realms'][realm]['metrics'].items():
            output.append((metric, minfo['text'] + ': ' + minfo['info']))
        return output

    def dimensions(self, realm):
        info = self.get_descriptor()
        output = []
        for dimension, dinfo in info['realms'][realm]['dimensions'].items():
            output.append((dimension, dinfo['text'] + ': ' + dinfo['info']))
        return output

    def get_descriptor(self):
        if self.descriptor:
            return self.descriptor

        self.crl.setopt(pycurl.URL,
                        self.xdmodhost + '/controllers/metric_explorer.php')
        config = {'operation': 'get_dw_descripter'}
        pf = urlencode(config)
        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.POSTFIELDS, pf)
        self.crl.perform()

        get_body = b_obj.getvalue()

        response = json.loads(get_body.decode('utf8'))

        if response['totalCount'] != 1:
            raise RuntimeError('Retrieving XDMoD data descriptor')

        self.descriptor = response['data'][0]

        return self.descriptor

    def timeseries(self, realm, dimension, metric, start, end):
        """ Undergoing prototype testing at the moment """

        config = {
            'start_date': start,
            'end_date': end,
            'realm': realm,
            'statistic': metric,
            'group_by': dimension,
            'public_user': 'true',
            'timeframe_label': '2016',
            'scale': '1',
            'aggregation_unit': 'Auto',
            'dataset_type': 'timeseries',
            'thumbnail': 'n',
            'query_group': 'po_usage',
            'display_type': 'line',
            'combine_type': 'side',
            'limit': '10',
            'offset': '0',
            'log_scale': 'n',
            'show_guide_lines': 'y',
            'show_trend_line': 'y',
            'show_percent_alloc': 'n',
            'show_error_bars': 'y',
            'show_aggregate_labels': 'n',
            'show_error_labels': 'n',
            'show_title': 'y',
            'width': '916',
            'height': '484',
            'legend_type': 'bottom_center',
            'font_size': '3',
            'inline': 'n',
            'operation': 'get_data',
            'format': 'csv'
        }

        response = self.get_usagedata(config)
        print(response)


        csvdata = csv.reader(response.splitlines())

        labelre = re.compile(r'\[([^\]]+)\].*')
        timestamps = []
        data = []
        for line_num, line in enumerate(csvdata):
            if line_num == 1:
                title = line[0]
            elif line_num == 5:
                start, end = line
            elif line_num == 7:
                timeunit = line[0]
                dimensions = []
                for label in line[1:]:
                    match = labelre.match(label)
                    if match:
                        dimensions.append(html.unescape(match.group(1)))
                    else:
                        dimensions.append(html.unescape(label))
            elif line_num > 7 and len(line) > 1:
                if re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", line[0]):
                    timestamps.append(datetime.strptime(line[0], "%Y-%m-%d"))
                    data.append(numpy.asarray(line[1:], dtype=numpy.float64))
                elif re.match(r"^[0-9]{4}-[0-9]{2}$", line[0]):
                    timestamps.append(datetime.strptime(line[0], "%Y-%m"))
                    data.append(numpy.asarray(line[1:], dtype=numpy.float64))
                else:
                    # TODO handle other date cases
                    raise Exception("Unsupported date specification " + line[0])

        return pd.DataFrame(data=data, index=timestamps, columns=dimensions)

    def aggregate(self, realm, dimension, metric, start, end):

        config = {
            'start_date': start,
            'end_date': end,
            'realm': realm,
            'statistic': metric,
            'group_by': dimension,
            'public_user': 'true',
            'timeframe_label': '2016',
            'scale': '1',
            'aggregation_unit': 'Auto',
            'dataset_type': 'aggregate',
            'thumbnail': 'n',
            'query_group': 'po_usage',
            'display_type': 'line',
            'combine_type': 'side',
            'limit': '10',
            'offset': '0',
            'log_scale': 'n',
            'show_guide_lines': 'y',
            'show_trend_line': 'y',
            'show_percent_alloc': 'n',
            'show_error_bars': 'y',
            'show_aggregate_labels': 'n',
            'show_error_labels': 'n',
            'show_title': 'y',
            'width': '916',
            'height': '484',
            'legend_type': 'bottom_center',
            'font_size': '3',
            'inline': 'n',
            'operation': 'get_data',
            'format': 'csv'
        }

        response = self.get_usagedata(config)
        csvdata = csv.reader(response.splitlines())

        return self.xdmodcsvtopandas(csvdata)

    def get_usagedata(self, config):

        self.crl.setopt(pycurl.URL,
                        self.xdmodhost + '/controllers/user_interface.php')
        pf = urlencode(config)
        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.POSTFIELDS, pf)
        self.crl.perform()

        get_body = b_obj.getvalue()

        return get_body.decode('utf8')

    def getjobs(self, start_date, end_date, count=50,start=0):

        config = {
            'realm': 'SUPREMM',
            'start_date': start_date,
            'end_date': end_date,
            'params': json.dumps({"resource":["1"]}),
            'limit': count,
            'start': start
        }

        self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/search/jobs?' + urlencode(config))

        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.HTTPGET, 1)
        self.crl.perform()

        get_body = b_obj.getvalue()

        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        if code != 200:
           raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

        result = json.loads(get_body.decode('utf8'))

        totalCount = count

        if 'totalCount' in result:
            totalCount = int(result['totalCount'])

        jobids = []

        for resdata in result['results']:
            jobids.append(resdata['jobid'])

        return totalCount,jobids

    def jobaccountingdata(self,jobid):
        
        config = {
            'realm': 'SUPREMM',
            'jobid': jobid,
            'recordid': 8,
            'infoid': 0
            }

        self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/search/jobs/accounting?' + urlencode(config))

        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.HTTPGET, 1)
        self.crl.perform()

        get_body = b_obj.getvalue()

        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        if code != 200:
           raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

        result = json.loads(get_body.decode('utf8'))

        data = dict()

        resdata = result['data']

        for keyvalpair in resdata:
            data[keyvalpair['key']] = keyvalpair['value']

        #fetch the hosts that this job ran on
        job_hosts = self.jobhostdata(jobid)
        data['Hosts'] = ','.join(job_hosts)

        return data

    def jobperformancedata(self,jobid):
        
        config = {
            'realm': 'SUPREMM',
            'jobid': jobid,
            }

        self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/search/jobs/detailedmetrics?' + urlencode(config))

        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.HTTPGET, 1)
        self.crl.perform()

        get_body = b_obj.getvalue()

        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        if code != 200:
           raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

        result = json.loads(get_body.decode('utf8'))

        return result

    def jobhostdata(self,jobid):
        
        config = {
            'realm': 'SUPREMM',
            'jobid': jobid,
            }

        self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/search/jobs/executable?' + urlencode(config))

        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.HTTPHEADER, self.headers)
        self.crl.setopt(pycurl.HTTPGET, 1)
        self.crl.perform()

        get_body = b_obj.getvalue()

        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        if code != 200:
           raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

        result = json.loads(get_body.decode('utf8'))

        #determine the hosts
        hosts = []
        for row in result:
            if 'children' in row:
                for tree1 in row['children']:
                    if 'children' in tree1:
                        for tree2 in tree1['children']:
                            if 'key' in tree2:
                                if 'node' in tree2['key']:
                                    if 'children' in tree2:
                                        for tree3 in tree2['children']:
                                            if 'key' in tree3:
                                                if tree3['key'] == 'node':
                                                    hosts.append(tree3['value'])

        return hosts

    def jobtimeseries(self,jobid):
       
        metrics = ['cpuuser', 'membw', 'simdins', 'gpu_usage', 'clktks', 'memused_minus_diskcache', 'power', 'memused', 'process_mem_usage', 'ib_lnet', 'lnet', 'block', 'nfs']

        results = dict()

        results['jobid'] = jobid.rstrip()

        results['data'] = []

        for metric in metrics:
            config = {
                'realm': 'SUPREMM',
                'jobid': jobid.rstrip(),
                'tsid': metric
                }

            self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/search/jobs/timeseries?' + urlencode(config))

            b_obj = io.BytesIO()
            self.crl.setopt(pycurl.WRITEDATA, b_obj)
            self.crl.setopt(pycurl.HTTPHEADER, self.headers)
            self.crl.setopt(pycurl.HTTPGET, 1)
            self.crl.perform()

            get_body = b_obj.getvalue()

            code = self.crl.getinfo(pycurl.RESPONSE_CODE)
            if code != 200:
                return results
               #raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

            res = json.loads(get_body.decode('utf8'))

            #determine the time series data for this metric
            if 'data' in res:
                for data in res['data']:
                    # assuming only one value in the data array
                    if 'series' in data:
                        host_data = dict()
                        for host in data['series']:
                            host_data[host['name']] = host['data']
                        result = dict()
                        result['metric'] = metric
                        result['units'] = data['schema']['units']
                        result['data'] = host_data
                        results['data'].append(result)

        return results

    def rawdata(self, realm, start, end, filters, stats):

        config = {
            'realm': realm,
            'start_date': start,
            'end_date': end,
            'params': filters,
            'stats': stats
        }

    def rawdata(self, realm, start, end, filters, stats):

        config = {
            'realm': realm,
            'start_date': start,
            'end_date': end,
            'params': filters,
            'stats': stats
        }

        request = json.dumps(config)

        self.crl.setopt(pycurl.URL, self.xdmodhost + '/rest/v1/warehouse/rawdata')

        b_obj = io.BytesIO()
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        headers = self.headers + ["Accept: application/json", "Content-Type: application/json", "charset: utf-8"]
        self.crl.setopt(pycurl.HTTPHEADER, headers)
        self.crl.setopt(pycurl.POSTFIELDS, request)
        self.crl.perform()

        get_body = b_obj.getvalue()

        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        if code != 200:
           raise RuntimeError('Error ' + str(code) + ' ' + get_body.decode('utf8'))

        result = json.loads(get_body.decode('utf8'))
        return pd.DataFrame(result['data'], columns=result['stats'], dtype=numpy.float64)

    def xdmodcsvtopandas(self, rd):
        groups = []
        data = []
        for line_num, line in enumerate(rd):
            if line_num == 1:
                title = line[0]
            elif line_num == 5:
                start, end = line
            elif line_num == 7:
                group, metric = line
            elif line_num > 7 and len(line) > 1:
                groups.append(html.unescape(line[0]))
                data.append(numpy.float64(line[1]))

        return pd.DataFrame(data=data, index=groups, columns=[metric, ])
